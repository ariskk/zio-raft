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

  private[raft] def getAllEntries = storage.log.getEntries(Index(0))

  private[raft] def sendMessage(m: Message): USTM[Unit] =
    queues.outboundQueue.offer(m).unit

  private[raft] def sendMessages(ms: Iterable[Message]): USTM[Unit] =
    queues.outboundQueue.offerAll(ms).unit

  private[raft] def nodeState: UIO[NodeState] = state.nodeState.commit

  private[raft] def isLeader: UIO[Boolean] = state.isLeader.commit

  private[raft] def peers = state.peerList

  private def sendAppendEntries = for {
    currentTerm <- storage.getTerm
    peers       <- state.peerList
    commitIndex <- state.lastCommitIndex
    logSize     <- storage.logSize
    _ <- ZSTM.collectAll(
      peers.map(p =>
        for {
          nextIndex <- state.nextIndexForPeer(p).map(_.getOrElse(Index(0)))
          previousIndex = if (nextIndex == Index(-1)) nextIndex else nextIndex.decrement
          previousTerm <-
            storage.getEntry(previousIndex).map(_.map(_.term).getOrElse(Term(-1)))
          entries <- storage.getEntries(nextIndex)
          _ <- sendMessage(
            AppendEntries[T](
              AppendEntries.newUniqueId,
              nodeId,
              p,
              currentTerm,
              prevLogIndex = previousIndex,
              prevLogTerm = previousTerm,
              leaderCommitIndex = commitIndex,
              entries = entries
            )
          )
        } yield ()
      )
    )
  } yield ()

  /**
   * If there exists an N such that N > commitIndex, a majority
   * of matchIndex[i] ≥ N, and log[N].term == currentTerm:
   * set commitIndex = N (§5.3, §5.4).
   */
  private def updateCommitIndex = for {
    term            <- storage.getTerm
    lastCommitIndex <- state.lastCommitIndex
    lastIndex       <- storage.lastIndex
    indices         <- state.matchIndexEntries.map(_.map { case (_, index) => index } :+ lastIndex)
    maybeMedian = indices.sortBy(_.index).lift(indices.size / 2)
    _ <- maybeMedian.fold[ZSTM[Any, StorageException, Unit]](ZSTM.unit) { median =>
      for {
        logEntry <- storage.getEntry(median)
        _ <- logEntry.fold[ZSTM[Any, StorageException, Unit]](ZSTM.unit) { entry =>
          if (entry.term == term && median > lastCommitIndex) state.updateCommitIndex(median)
          else ZSTM.unit
        }
      } yield ()
    }
  } yield ()

  /**
   * AppendEntriesResponses can arrive out of order
   */
  private def handleAppendEntriesResponse(response: AppendEntriesResponse) =
    if (response.success)
      state.updateMatchIndex(response.from, response.lastInsertedIndex) *>
        state.updateNextIndex(response.from, response.lastInsertedIndex.increment) *>
        updateCommitIndex
    else state.decrementNextIndex(response.from)

  private def processAppendEntriesResponses = queues.appendResponseQueue.takeAll.commit.flatMap { aers =>
    for {
      currentTerm <- storage.getTerm.commit
      maxTerm = aers.map(_.term.term).maxOption
      _ <- maxTerm.fold[ZIO[Clock, StorageException, Unit]](IO.unit) { mx =>
        if (mx > currentTerm.term)
          (storage.storeTerm(Term(mx)) <*
            state.becomeFollower).commit
        // Potential optimisation using collectAllPar
        else
          ZIO
            .collectAll(
              aers.map(response => handleAppendEntriesResponse(response).commit)
            )
            .unit
      }
    } yield ()
  }.repeatWhileM(_ => isLeader)

  /**
   * Collects the acknowledgements as they come. Steps down if it finds a later term.
   * Sends heartbeats every 50 milliseconds until it steps down.
   */
  private def sendHeartbeats: ZIO[Clock, RaftException, Unit] = {

    lazy val sendHeartbeatEntries = sendAppendEntries.commit
      .delay(Raft.LeaderHeartbeat)
      .repeatWhileM(_ => isLeader)

    (processAppendEntriesResponses &> sendHeartbeatEntries) *> processInboundEntries

  }

  private def sendVoteRequests(term: Term): STM[StorageException, Unit] = for {
    peers     <- state.peerList
    lastIndex <- storage.lastIndex
    lastTerm  <- storage.lastEntry.map(_.map(_.term).getOrElse(Term(-1)))
    requests = peers.map(p => VoteRequest(state.nodeId, p, term, lastIndex, lastTerm))
    _ <- sendMessages(requests)
  } yield ()

  private def collectVotes(forTerm: Term) = {
    lazy val voteCollectionProgram = for {
      newVote <- queues.inboundVoteResponseQueue.take.commit
      processVote =
        if (newVote.term.isAfter(forTerm))
          storage.storeTerm(newVote.term) *>
            state.becomeFollower
        else if (newVote.term == forTerm && newVote.granted)
          state.addVote(Vote(newVote.from, newVote.term))
        else if (newVote.term == forTerm && !newVote.granted)
          state.addVoteRejection(Vote(newVote.from, newVote.term))
        else ZSTM.unit
      _ <- processVote.commit
    } yield ()

    voteCollectionProgram
      .repeatUntilM(_ => nodeState.map(_ != NodeState.Candidate))
  }

  private def electionResult: ZIO[clock.Clock, RaftException, Unit] = for {
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
      _ <- state.stand(newTerm)
      _ <- storage.storeTerm(newTerm)
      _ <- storage.storeVote(Vote(nodeId, newTerm))
      _ <- sendVoteRequests(newTerm)
    } yield newTerm

    standForElectionProgram.commit.flatMap { term =>
      collectVotes(term) *> electionResult
    }
  }

  private def sendVoteResponse(voteRequest: VoteRequest, granted: Boolean) =
    sendMessage(VoteResponse(nodeId, voteRequest.from, voteRequest.term, granted))

  private def proccessVoteRequest(voteRequest: VoteRequest) = (for {
    (currentTerm, currentVote) <- storage.getTerm <*> storage.getVote
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

  private def processInboundVoteRequests: ZIO[Clock, RaftException, Unit] = queues.inboundVoteRequestQueue.take.commit
    .flatMap(proccessVoteRequest)
    .forever

  private def shouldAppend(previousIndex: Index, previousTerm: Term) =
    if (previousIndex.index == -1) STM.succeed(true)
    else
      for {
        size <- storage.logSize
        term <- storage.getEntry(previousIndex).map(_.map(_.term).getOrElse(Term.Invalid))

      } yield previousTerm == term

  /**
   * TODO
   *  If an existing entry conflicts with a new one (same index
   *    but different terms), delete the existing entry and all that
   *    follow it (§5.3) also commit index and lastApplied
   */
  private def appendLog(ae: AppendEntries[T]) =
    shouldAppend(ae.prevLogIndex, ae.prevLogTerm).flatMap { shouldAppend =>
      if (shouldAppend) for {
        _             <- storage.appendEntries(ae.entries.toList)
        currentCommit <- state.lastCommitIndex
        _ <-
          if (ae.leaderCommitIndex > currentCommit)
            storage.logSize.flatMap { size =>
              state.updateCommitIndex(Index(math.min(size - 1, ae.leaderCommitIndex.index)))
            }
          else STM.unit
      } yield ()
      else STM.unit
    }

  private def processEntries(ae: AppendEntries[T]) = (for {
    currentTerm  <- storage.getTerm
    currentState <- state.nodeState
    lastIndex    <- storage.logSize.map(x => Index(x - 1))
    (success, term) <-
      if (ae.term.isAfter(currentTerm))
        storage.storeTerm(ae.term) *>
          state.becomeFollower *>
          state.setLeader(ae.from) *>
          appendLog(ae).map(_ => (true, ae.term))
      else if (ae.term == currentTerm && currentState != NodeState.Follower)
        state.becomeFollower *>
          state.setLeader(ae.from) *>
          appendLog(ae).map(_ => (true, ae.term))
      else if (ae.term == currentTerm && currentState == NodeState.Follower)
        state.setLeader(ae.from) *>
          appendLog(ae).map(_ => (true, ae.term))
      else if (currentTerm.isAfter(ae.term)) STM.succeed((false, currentTerm))
      else STM.fail(InvalidStateException("Unknown state. This is likely a bug."))
    updatedLastIndex <- storage.logSize.map(x => Index(x - 1))
    _ <- sendMessage(
      AppendEntriesResponse(
        ae.to,
        ae.from,
        ae.appendId,
        term,
        lastIndex,
        updatedLastIndex,
        success = success
      )
    )
  } yield ()).commit

  private def processInboundEntries: ZIO[Clock, RaftException, Unit] =
    queues.appendEntriesQueue.take.commit.disconnect
      .timeout(ElectionTimeout.newTimeout.value.milliseconds)
      .flatMap { maybeEntries =>
        maybeEntries.fold[ZIO[Clock, RaftException, Unit]](
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

  private def processCommand(command: T) = {
    lazy val processCommandProgram = for {
      term    <- storage.getTerm
      _       <- storage.appendEntry(LogEntry(command, term))
      logSize <- storage.logSize

      // This will be executed on the next heartbeat
      //_ <- sendAppendEntries
    } yield logSize

    processCommandProgram.commit.flatMap { logSize =>
      state.lastCommitIndex.commit
        .repeatUntil(_.index >= logSize - 1)
        .map(_ => Committed)
    }
  }

  def submitCommand(command: T): ZIO[Clock, RaftException, CommandResponse] =
    state.leader.commit.flatMap { leader =>
      leader match {
        case Some(leaderId) if leaderId == nodeId => processCommand(command)
        case Some(leaderId) if leaderId != nodeId => ZIO.succeed(Redirect(leaderId))
        case None                                 => ZIO.fail(LeaderNotFoundException)
      }
    }

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
