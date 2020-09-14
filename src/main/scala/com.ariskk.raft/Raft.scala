package com.ariskk.raft

import zio._
import zio.duration._
import zio.stm._

import com.ariskk.raft.model._
import Message._
import Raft.State

// TODO replace UIO[Unit] with type EIO = IO[Error, Unit]
// TODO check if this can become ReaderT[NodeDeps, ZIO, Out] and this becomes an object
// TODO ZIO is a reader. 
/**
 * Runs the consensus module of a single Raft Node.
 * It communicates with the outside world through message passing, using `TQueue` instances.
 * Communication with the outside world is asynchronous.
*/
final class Raft(state: State) {

  def poll: UIO[Message] = state.outboundQueue.take.forever.commit

  def offerVote(vote: VoteResponse) = state.inboundVoteResponseQueue.offer(vote).commit

  def offerHeartbeatAck(ack: HeartbeatAck)= state.inboundHeartbeatAckQueue.offer(ack).commit

  private[raft] def sendMessage(m: Message): USTM[Unit] = state.outboundQueue.offer(m).unit

  private[raft] def sendMessages(ms: Iterable[Message]): USTM[Unit] = state.outboundQueue.offerAll(ms).unit

  private[raft] def becomeCandidate: USTM[RaftNode] = state.raftNode.updateAndGet(_.stand)

  private[raft] def node: UIO[RaftNode] = state.raftNode.get.commit

  private[raft] def nodeState: UIO[NodeState] = state.raftNode.get.commit.map(_.state)

  private[raft] def nodeId: USTM[RaftNode.Id] = state.raftNode.map(_.id).get

  private[raft] def isLeader: UIO[Boolean] = state.raftNode.get.commit.map(_.isLeader)

  private def sendHeartbeats = {
    val hearbeatProgram = for {
      currentNode <- state.raftNode.get
      ackMsgs <- state.inboundHeartbeatAckQueue.takeAll
      // if there are different terms here, which one should I pick?
      _ <- if (ackMsgs.exists(_.term.isAfter(currentNode.term))) {
        ZSTM.collectAll(
          ackMsgs.filter(_.term.isAfter(currentNode.term)).map(msg =>
            state.raftNode.update(_.becomeFollower(msg.term))
          )
        )  
      } else {
        val heartbeats = currentNode.peers.map(p => Heartbeat(currentNode.id, p, currentNode.term))
        sendMessages(heartbeats)
      }
    } yield ()
    
    hearbeatProgram
      .commit
      .delay(Raft.LeaderHeartbeat)
      .repeatUntilM(_ => isLeader.map(!_))
  }

  private def sendVoteRequests(candidate: RaftNode): USTM[Unit] = {
    val requests = candidate.peers.map(p =>
      VoteRequest(candidate.id, p, candidate.term)
    )
    sendMessages(requests)
  }

  private def collectVotes(forTerm: Term): UIO[Unit] = {
    lazy val voteCollectionProgram = for {
      maybeNewVote <- state.inboundVoteResponseQueue.poll
      currentNode <- state.raftNode.get
      _ <- maybeNewVote.fold(ZSTM.unit) { newVote =>
        if (newVote.term.isAfter(forTerm))
          state.raftNode.update(_.becomeFollower(newVote.term))
        else state.raftNode.set(
          currentNode.addVote(newVote.from, newVote.term)
        )    
      }
    } yield ()

    voteCollectionProgram
      .commit
      .repeatUntilM(_ =>
        nodeState.map(_ != NodeState.Candidate)
      )
      .unit
  }

  private def electionResult: ZIO[Any with clock.Clock, InvalidStateException, Unit] = for {
    state <- nodeState
    _ <- state match {
      case NodeState.Follower => runFollowerLoop
      case NodeState.Candidate => 
        ZIO.die(InvalidStateException("Node still in Candidate state after election has concluded"))
      case NodeState.Leader => sendHeartbeats
    }
  } yield ()

  def runForLeader =  {
    lazy val standForElectionProgram = for {
      candidate <- becomeCandidate
      _ <- sendVoteRequests(candidate)
    } yield candidate.term

    standForElectionProgram.commit.flatMap { term =>
      collectVotes(term) *> electionResult
    }

  }

  def sendVoteResponse(voteRequest: VoteRequest, granted: Boolean) = for {
    fromId <- nodeId
    response = VoteResponse(fromId, voteRequest.from, voteRequest.term, granted)
    _ <- sendMessage(response)
  } yield ()

  def proccessVoteRequest(voteRequest: VoteRequest) = (for {
    currentNode <- state.raftNode.get
    nodeId = currentNode.id
    _ <- if (voteRequest.term.isAfter(currentNode.term))
      state.raftNode.update(_.becomeFollower(voteRequest.term)) *> 
        sendVoteResponse(voteRequest, granted = true)
    else if (
      voteRequest.term == currentNode.term && 
      (currentNode.votedFor.isEmpty || currentNode.votedFor.contains(voteRequest.from))
    ) sendVoteResponse(voteRequest, granted = true)
    // This is a bit of a problem will the the network layer be injected here to have this?
    else sendVoteResponse(voteRequest, granted = false)
  } yield ()).commit

  def runFollowerLoop = {
  
    lazy val followerProgram = state.heartbeatQueue.takeAll.commit.delay(
        ElectionTimeout.newTimeout.value.milliseconds
      ).repeatWhile(_.nonEmpty) 
    
    followerProgram *> runForLeader
  }
  
  def run = runFollowerLoop.forever.unit

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

    def apply(nodeId: RaftNode.Id, peers: Set[RaftNode.Id], queueSize: Int = DefaultQueueSize): UIO[State] = for {
      node <- TRef.makeCommit(RaftNode.initial(nodeId = nodeId, peers = peers))
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

  def default: UIO[Raft] =  State.default.map(d =>
    new Raft(d)
  )

  def apply(nodeId: RaftNode.Id, peers: Set[RaftNode.Id]) = State(nodeId, peers).map(d =>
    new Raft(d)
  )

}