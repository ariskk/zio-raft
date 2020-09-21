package com.ariskk.raft

import zio._
import zio.duration._
import zio.stm._
import zio.clock._

import com.ariskk.raft.model._
import Message._
import Raft.State

/**
 * Runs the consensus module of a single Raft Node.
 * It communicates with the outside world through message passing, using `TQueue` instances.
 * Communication with the outside world is asynchronous; all messages are fire and forget.
*/
final class Raft(val nodeId: RaftNode.Id, state: State) {

  def poll: UIO[Option[Message]] = state.outboundQueue.poll.commit

  def takeAll: UIO[Seq[Message]] = state.outboundQueue.takeAll.commit

  def offerVote(vote: VoteResponse) = state.inboundVoteResponseQueue.offer(vote).commit

  def offerHeartbeatAck(ack: HeartbeatAck) = state.inboundHeartbeatAckQueue.offer(ack).commit

  def offerHeartbeat(h: Heartbeat) = state.heartbeatQueue.offer(h).commit

  def offerVoteRequest(request: VoteRequest) = state.inboundVoteRequestQueue.offer(request).commit

  def addPeer(peer: RaftNode.Id) = state.raftNode.update(_.addPeer(peer)).commit

  def removePeer(peer: RaftNode.Id) = state.raftNode.update(_.removePeer(peer)).commit

  private[raft] def sendMessage(m: Message): USTM[Unit] =
    state.outboundQueue.offer(m).unit

  private[raft] def sendMessages(ms: Iterable[Message]): USTM[Unit] =
    state.outboundQueue.offerAll(ms).unit

  private[raft] def becomeCandidate: USTM[RaftNode] = state.raftNode.updateAndGet(_.stand)

  private[raft] def node: UIO[RaftNode] = state.raftNode.get.commit

  private[raft] def nodeState: UIO[NodeState] = state.raftNode.get.commit.map(_.state)

  private[raft] def isLeader: UIO[Boolean] = state.raftNode.get.commit.map(_.isLeader)

  /**
   * Collects the acknowledgements as they come. Steps down if it finds a later term.
   * Sends heartbeats every 50 milliseconds until it steps down.
  */
  private def sendHeartbeats: RIO[Clock, Unit] = {
    lazy val collectAcks = state
      .inboundHeartbeatAckQueue
      .takeAll
      .commit
      .flatMap { acks =>
        for {
          currentNode <- state.raftNode.get.commit
          maxTerm = acks.map(_.term.term).maxOption
          _ <- maxTerm.fold(ZIO.unit) { mx =>
            if (mx > currentNode.term.term)
              state.raftNode.update(_.becomeFollower(Term(mx))).commit
            else ZIO.unit
          }
        } yield ()
      }.repeatWhileM(_ => isLeader)

    lazy val sendHeartbeats = state.raftNode.get.commit.flatMap { currentNode =>
      sendMessages(
        currentNode.peers.map(p => Heartbeat(currentNode.id, p, currentNode.term))
      ).commit
    }.delay(Raft.LeaderHeartbeat).repeatWhileM(_ => isLeader)

    (collectAcks &> sendHeartbeats) *> processInboundHeartbeats

  }

  private def sendVoteRequests(candidate: RaftNode): USTM[Unit] = {
    val requests = candidate.peers.map(p =>
      VoteRequest(candidate.id, p, candidate.term)
    )
    sendMessages(requests)
  }

  private def collectVotes(forTerm: Term) = {
    lazy val voteCollectionProgram = for {
      newVote <- state.inboundVoteResponseQueue.take
      currentNode <- state.raftNode.get
      _ <- if (newVote.term.isAfter(forTerm))
          state.raftNode.update(_.becomeFollower(newVote.term))
        else if (newVote.term == forTerm && newVote.granted) 
          state.raftNode.set(
            currentNode.addVote(newVote.from, newVote.term)
          )
        else if (newVote.term == forTerm && !newVote.granted)
          state.raftNode.set(
            currentNode.addVoteRejection(newVote.from, newVote.term)
          )
        else ZSTM.unit
    } yield ()

    voteCollectionProgram
      .commit
      .repeatUntilM(_ =>
        nodeState.map(_ != NodeState.Candidate)
      )
  }

  private def electionResult: ZIO[clock.Clock, Throwable, Unit] = for {
    state <- nodeState
    _ <- state match {
      case NodeState.Follower => processInboundHeartbeats
      case NodeState.Candidate => 
        ZIO.die(InvalidStateException("Node still in Candidate state after election has concluded"))
      case NodeState.Leader => sendHeartbeats
    }
  } yield ()

  private[raft] def runForLeader =  {
    lazy val standForElectionProgram = for {
      candidate <- becomeCandidate.commit
      _ <- sendVoteRequests(candidate).commit
    } yield candidate.term

    standForElectionProgram.flatMap { term =>
      collectVotes(term) *> electionResult
    }
  }

  private def sendVoteResponse(voteRequest: VoteRequest, granted: Boolean) =
    sendMessage(VoteResponse(nodeId, voteRequest.from, voteRequest.term, granted))

  private def proccessVoteRequest(voteRequest: VoteRequest) = (for {
    currentNode <- state.raftNode.get
    _ <- if (voteRequest.term.isAfter(currentNode.term))
      state.raftNode.update(_.becomeFollower(voteRequest.term).voteFor(voteRequest.from)) *> 
       sendVoteResponse(voteRequest, granted = true)
    else if (
      voteRequest.term == currentNode.term && 
      (currentNode.votedFor.isEmpty || currentNode.votedFor.contains(voteRequest.from))
    ) state.raftNode.update(_.voteFor(voteRequest.from)) *>
      sendVoteResponse(voteRequest, granted = true)
    else sendVoteResponse(voteRequest, granted = false)
  } yield ()).commit

  private def processInboundVoteRequests = state
    .inboundVoteRequestQueue
    .take
    .commit
    .flatMap(proccessVoteRequest)
    .forever

  private def processHeartbeat(hb: Heartbeat) = (
    for {
      node <- state.raftNode.get
      term <- if (hb.term.isAfter(node.term)) 
          state.raftNode.update(_.becomeFollower(hb.term)).map(_ =>
            hb.term
          )
        else STM.succeed(node.term)
      _ <- sendMessage(HeartbeatAck(hb.to, hb.from, term))
    } yield ()).commit

  private def processInboundHeartbeats: RIO[Clock, Unit] =
    state.heartbeatQueue
      .take
      .commit
      .disconnect
      .timeout(ElectionTimeout.newTimeout.value.milliseconds)
      .flatMap { maybeHeartbeat =>
        maybeHeartbeat.fold(
          for {
            node <- state.raftNode.get.commit
            _ <- if ((node.isFollower && node.votedFor.isEmpty) || node.hasLost(node.term)) runForLeader
              else processInboundHeartbeats
          } yield ()
        )(
          hb => processHeartbeat(hb) *> processInboundHeartbeats
        )
      }

  private[raft] def runFollowerLoop = 
    processInboundVoteRequests <&> processInboundHeartbeats
  
  def run = runFollowerLoop

}

object Raft {

  case class State(
    raftNode: TRef[RaftNode],
    inboundVoteResponseQueue: TQueue[VoteResponse],
    inboundVoteRequestQueue: TQueue[VoteRequest],
    inboundHeartbeatAckQueue: TQueue[HeartbeatAck],
    heartbeatQueue: TQueue[Heartbeat],
    outboundQueue: TQueue[Message]
  )

  object State  {
    private val DefaultQueueSize = 100

    def default: UIO[State] = apply(RaftNode.newUniqueId, Set.empty, DefaultQueueSize)

    private def newQueue[T](queueSize: Int) = 
      TQueue.bounded[T](queueSize).commit

    def apply(
      nodeId: RaftNode.Id,
      peers: Set[RaftNode.Id],
      queueSize: Int = DefaultQueueSize
    ): UIO[State] = for {
      node <- TRef.makeCommit(RaftNode.initial(nodeId, peers))
      inboundVoteResponseQueue <- newQueue[VoteResponse](queueSize)
      inboundVoteRequestQueue <- newQueue[VoteRequest](queueSize)
      inboundHeartbeatAckQueue <- newQueue[HeartbeatAck](queueSize)
      heartbeatQueue <- newQueue[Heartbeat](queueSize)
      outboundQueue <- newQueue[Message](queueSize)
    } yield State(
      node,
      inboundVoteResponseQueue,
      inboundVoteRequestQueue,
      inboundHeartbeatAckQueue,
      heartbeatQueue,
      outboundQueue
    )
  }

  val LeaderHeartbeat = 50.milliseconds

  def default: UIO[Raft] =  {
    val id = RaftNode.newUniqueId
    State(id, Set.empty).map(d =>
      new Raft(id, d)
    )
  }

  def apply(nodeId: RaftNode.Id, peers: Set[RaftNode.Id]) = State(nodeId, peers).map(state =>
    new Raft(nodeId, state)
  )

}