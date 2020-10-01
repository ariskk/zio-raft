package com.ariskk.raft

import zio._
import zio.duration._
import zio.stm._
import zio.clock._

import com.ariskk.raft.model._
import Message._
import Raft.{ MessageQueues }
import com.ariskk.raft.state.VolatileState
import com.ariskk.raft.storage.Storage

/**
 * Runs the consensus module of a single Raft Node.
 * It communicates with the outside world through message passing, using `TQueue` instances.
 * Communication with the outside world is asynchronous; all messages are fire and forget.
 */
final class Raft[T](
  val nodeId: NodeId,
  state: VolatileState,
  storage: Storage[T],
  queues: MessageQueues[T]
) {

  def poll: UIO[Option[Message]] = queues.outboundQueue.poll.commit

  def takeAll: UIO[Seq[Message]] = queues.outboundQueue.takeAll.commit

  def offerVote(vote: VoteResponse) = queues.inboundVoteResponseQueue.offer(vote).commit

  def offerAppendEntriesResponse(r: AppendEntriesResponse) = queues.appendResponseQueue.offer(r).commit

  def offerAppendEntries(entries: AppendEntries[T]) = queues.appendEntriesQueue.offer(entries).commit

  def offerVoteRequest(request: VoteRequest) = queues.inboundVoteRequestQueue.offer(request).commit

  def addPeer(peer: NodeId) = state.addPeer(peer).commit

  def removePeer(peer: NodeId) = state.removePeer(peer).commit

  private[raft] def sendMessage(m: Message): USTM[Unit] =
    queues.outboundQueue.offer(m).unit

  private[raft] def sendMessages(ms: Iterable[Message]): USTM[Unit] =
    queues.outboundQueue.offerAll(ms).unit

  private[raft] def nodeState: UIO[NodeState] = state.nodeState.commit

  private[raft] def isLeader: UIO[Boolean] = state.isLeader.commit

  private[raft] def peers = state.peerList

  /**
   * Collects the acknowledgements as they come. Steps down if it finds a later term.
   * Sends heartbeats every 50 milliseconds until it steps down.
   */
  private def sendHeartbeats: ZIO[Clock, StorageException, Unit] = {
    lazy val collectAcks = queues.appendResponseQueue.takeAll.commit.flatMap { acks =>
      for {
        currentTerm <- storage.getTerm.commit
        maxTerm = acks.map(_.term.term).maxOption
        _ <- maxTerm.fold[ZIO[Clock, StorageException, Unit]](IO.unit) { mx =>
          if (mx > currentTerm.term)
            (storage.storeTerm(Term(mx)) <*
              state.becomeFollower).commit
          else IO.unit
        }
      } yield ()
    }.repeatWhileM(_ => isLeader)

    lazy val sendHeartbeatEntries = (for {
      currentTerm <- storage.getTerm.commit
      peers       <- state.peerList.commit
      _ <- sendMessages(
        peers.map(p =>
          AppendEntries[T](
            AppendEntries.newUniqueId,
            state.nodeId,
            p,
            currentTerm,
            prevLogIndex = Index(-1),
            prevLogTerm = Term(-1),
            leaderCommitIndex = Index(-1),
            entries = Seq.empty
          )
        )
      ).commit

    } yield ()).delay(Raft.LeaderHeartbeat).repeatWhileM(_ => isLeader)

    (collectAcks &> sendHeartbeatEntries) *> processInboundEntries

  }

  private def sendVoteRequests: STM[StorageException, Unit] = for {
    peers <- state.peerList
    term  <- storage.getTerm
    requests = peers.map(p => VoteRequest(state.nodeId, p, term))
    _ <- sendMessages(requests)
  } yield ()

  private def collectVotes(forTerm: Term) = {
    lazy val voteCollectionProgram = for {
      newVote <- queues.inboundVoteResponseQueue.take
      _ <-
        if (newVote.term.isAfter(forTerm))
          storage.storeTerm(newVote.term) *>
            state.becomeFollower
        else if (newVote.term == forTerm && newVote.granted)
          state.addVote(Vote(newVote.from, newVote.term))
        else if (newVote.term == forTerm && !newVote.granted)
          state.addVoteRejection(Vote(newVote.from, newVote.term))
        else ZSTM.unit
    } yield ()

    voteCollectionProgram.commit
      .repeatUntilM(_ => nodeState.map(_ != NodeState.Candidate))
  }

  private def electionResult: ZIO[clock.Clock, StorageException, Unit] = for {
    state <- nodeState
    _ <- state match {
      case NodeState.Follower => processInboundEntries
      case NodeState.Candidate =>
        ZIO.die(InvalidStateException("Node still in Candidate state after election has concluded"))
      case NodeState.Leader => sendHeartbeats
    }
  } yield ()

  private[raft] def runForLeader = {
    lazy val standForElectionProgram = for {
      currentTerm <- storage.getTerm
      newTerm = currentTerm.increment
      _    <- state.stand(newTerm)
      _    <- storage.storeTerm(newTerm)
      _    <- sendVoteRequests
      term <- storage.getTerm
    } yield term

    standForElectionProgram.commit.flatMap { term =>
      collectVotes(term) *> electionResult
    }
  }

  private def sendVoteResponse(voteRequest: VoteRequest, granted: Boolean) =
    sendMessage(VoteResponse(nodeId, voteRequest.from, voteRequest.term, granted))

  private def proccessVoteRequest(voteRequest: VoteRequest) = (for {
    currentTerm <- storage.getTerm
    currentVote <- storage.getVote
    _ <-
      if (voteRequest.term.isAfter(currentTerm))
        state.becomeFollower *> 
          storage.storeTerm(voteRequest.term) *>
            storage.storeVote(Vote(voteRequest.from, voteRequest.term)) *>
              sendVoteResponse(voteRequest, granted = true)
      else if (
        voteRequest.term == currentTerm &&
        (currentVote.isEmpty || currentVote.contains(Vote(voteRequest.from, currentTerm)))
      )
        storage.storeVote(Vote(voteRequest.from, currentTerm)) *>
          sendVoteResponse(voteRequest, granted = true)
      else sendVoteResponse(voteRequest, granted = false)
  } yield ()).commit

  private def processInboundVoteRequests = queues.inboundVoteRequestQueue.take.commit
    .flatMap(proccessVoteRequest)
    .forever

  private def processEntries(ae: AppendEntries[T]) = (for {
    currentTerm <- storage.getTerm
    term <-
      if (ae.term.isAfter(currentTerm))
        storage.storeTerm(ae.term) *>
          state.becomeFollower.map(_ => ae.term)
      else STM.succeed(currentTerm)
    _ <- sendMessage(AppendEntriesResponse(ae.to, ae.from, ae.appendId, term, success = true))
  } yield ()).commit

  private def processInboundEntries: ZIO[Clock, StorageException, Unit] =
    queues.appendEntriesQueue.take.commit.disconnect
      .timeout(ElectionTimeout.newTimeout.value.milliseconds)
      .flatMap { maybeEntries =>
        maybeEntries.fold[ZIO[Clock, StorageException, Unit]](
          for {
            nodeState   <- state.nodeState.commit
            currentTerm <- storage.getTerm.commit
            currentVote <- storage.getVote.commit
            hasLost     <- state.hasLost(currentTerm).commit
            _ <-
              if ((nodeState == NodeState.Follower && currentVote.isEmpty) || hasLost) runForLeader
              else processInboundEntries
          } yield ()
        )(es => processEntries(es) *> processInboundEntries)
      }

  private[raft] def runFollowerLoop =
    processInboundVoteRequests <&> processInboundEntries

  def run = runFollowerLoop

}

object Raft {

  case class MessageQueues[T](
    inboundVoteResponseQueue: TQueue[VoteResponse],
    inboundVoteRequestQueue: TQueue[VoteRequest],
    appendResponseQueue: TQueue[AppendEntriesResponse],
    appendEntriesQueue: TQueue[AppendEntries[T]],
    outboundQueue: TQueue[Message]
  )

  object MessageQueues {

    private val DefaultQueueSize = 100

    private def newQueue[T](queueSize: Int) =
      TQueue.bounded[T](queueSize).commit

    def default[T] = apply[T](DefaultQueueSize)

    def apply[T](queueSize: Int): UIO[MessageQueues[T]] = for {
      inboundVoteResponseQueue <- newQueue[VoteResponse](queueSize)
      inboundVoteRequestQueue  <- newQueue[VoteRequest](queueSize)
      appendResponseQueue      <- newQueue[AppendEntriesResponse](queueSize)
      appendEntriesQueue       <- newQueue[AppendEntries[T]](queueSize)
      outboundQueue            <- newQueue[Message](queueSize)
    } yield MessageQueues(
      inboundVoteResponseQueue,
      inboundVoteRequestQueue,
      appendResponseQueue,
      appendEntriesQueue,
      outboundQueue
    )
  }

  val LeaderHeartbeat = 50.milliseconds

  def default[T](storage: Storage[T]): UIO[Raft[T]] = {
    val id = NodeId.newUniqueId
    for {
      state  <- VolatileState(id, Set.empty[NodeId])
      queues <- MessageQueues.default[T]
    } yield new Raft[T](id, state, storage, queues)
  }

  def apply[T](nodeId: NodeId, peers: Set[NodeId], storage: Storage[T]) = for {
    state  <- VolatileState(nodeId, peers)
    queues <- MessageQueues.default[T]
  } yield new Raft[T](nodeId, state, storage, queues)

}
