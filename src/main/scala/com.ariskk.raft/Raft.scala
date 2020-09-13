package com.ariskk.raft

import zio._
import zio.duration._
import zio.stm._

import com.ariskk.raft.model._
import Message._
import Raft.Dependencies

// TODO replace UIO[Unit] with type EIO = IO[Error, Unit]
// TODO check if this can become ReaderT[NodeDeps, ZIO, Out] and this becomes an object
// TODO ZIO is a reader. 
/**
 * Runs the consensus module of a single Raft Node.
 * It communicates with the outside world through message passing, using `TQueue` instances.
 * Communication with the outside world is asynchronous.
*/
final class Raft(d: Dependencies) {

  def poll: UIO[Message] = d.outboundQueue.take.forever.commit

  def offerVote(vote: VoteResponse) = d.inboundVoteResponseQueue.offer(vote).commit

  def offerHeartbeatAck(ack: HeartbeatAck)= d.inboundHeartbeatAckQueue.offer(ack).commit

  private[raft] def sendMessage(m: Message): USTM[Unit] = d.outboundQueue.offer(m).unit

  private[raft] def sendMessages(ms: Iterable[Message]): USTM[Unit] = d.outboundQueue.offerAll(ms).unit

  private[raft] def becomeCandidate: USTM[RaftNode] = d.raftNode.updateAndGet(_.stand)

  private[raft] def node: UIO[RaftNode] = d.raftNode.get.commit

  private[raft] def nodeState: UIO[NodeState] = d.raftNode.get.commit.map(_.state)

  private[raft] def nodeId: USTM[RaftNode.Id] = d.raftNode.map(_.id).get

  private[raft] def isLeader: UIO[Boolean] = d.raftNode.get.commit.map(_.isLeader)

  private def sendHeartbeats = {
    val hearbeatProgram = for {
      currentNode <- d.raftNode.get
      ackMsgs <- d.inboundHeartbeatAckQueue.takeAll
      // if there are different terms here, which one should I pick?
      _ <- if (ackMsgs.exists(_.term.isAfter(currentNode.term))) {
        ZSTM.collectAll(
          ackMsgs.filter(_.term.isAfter(currentNode.term)).map(msg =>
            d.raftNode.update(_.becomeFollower(msg.term))
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
      maybeNewVote <- d.inboundVoteResponseQueue.poll
      currentNode <- d.raftNode.get
      _ <- maybeNewVote.fold(ZSTM.unit) { newVote =>
        if (newVote.term.isAfter(forTerm))
          d.raftNode.update(_.becomeFollower(newVote.term))
        else d.raftNode.set(
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
    currentNode <- d.raftNode.get
    nodeId = currentNode.id
    _ <- if (voteRequest.term.isAfter(currentNode.term))
      d.raftNode.update(_.becomeFollower(voteRequest.term)) *> 
        sendVoteResponse(voteRequest, granted = true)
    else if (
      voteRequest.term == currentNode.term && 
      (currentNode.votedFor.isEmpty || currentNode.votedFor.contains(voteRequest.from))
    ) sendVoteResponse(voteRequest, granted = true)
    // This is a bit of a problem will the the network layer be injected here to have this?
    else sendVoteResponse(voteRequest, granted = false)
  } yield ()).commit

  def runFollowerLoop = {
  
    lazy val followerProgram = d.heartbeatQueue.takeAll.commit.delay(
        ElectionTimeout.newTimeout.value.milliseconds
      ).repeatWhile(_.nonEmpty) 
    
    followerProgram *> runForLeader
  }
  
  def run = runFollowerLoop.forever.unit

}

object Raft {

  case class Dependencies(
    raftNode: TRef[RaftNode],
    inboundVoteResponseQueue: TQueue[VoteResponse],
    inboundVoteRequestQueue: TQueue[VoteRequest],
    inboundHeartbeatAckQueue: TQueue[HeartbeatAck],
    heartbeatQueue: TQueue[Heartbeat],
    outboundQueue: TQueue[Message]
  )

  object Dependencies  {
    private val DefaultQueueSize = 100

    def default: UIO[Dependencies] = apply(Set.empty, DefaultQueueSize)

    private def newQueue[T](queueSize: Int) = 
      TQueue.bounded[T](queueSize).commit

    def apply(peers: Set[RaftNode.Id], queueSize: Int = DefaultQueueSize): UIO[Dependencies] = for {
      node <- TRef.makeCommit(RaftNode.initial(peers))
      inboundVoteResponseQueue <- newQueue[VoteResponse](queueSize)
      inboundVoteRequestQueue <- newQueue[VoteRequest](queueSize)
      inboundHeartbeatAckQueue <- newQueue[HeartbeatAck](queueSize)
      heartbeatQueue <- newQueue[Heartbeat](queueSize)
      outboundQueue <- newQueue[Message](queueSize)
    } yield Dependencies(
      node,
      inboundVoteResponseQueue,
      inboundVoteRequestQueue,
      inboundHeartbeatAckQueue,
      heartbeatQueue,
      outboundQueue
    )
  }

  val LeaderHeartbeat = 50.milliseconds

  def default: UIO[Raft] =  Dependencies.default.map(d =>
    new Raft(d)
  )

  def apply(peers: Set[RaftNode.Id]) = Dependencies(peers).map(d =>
    new Raft(d)
  )

}