package com.ariskk.raft

import zio._
import zio.duration._
import zio.stm._

import com.ariskk.raft.model.{
  RaftNode,
  Message,
  NodeState,
  ElectionTimeout
}
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

  // last heartbeat ref -> gets update on every heartbeat
  // a process wakes up every  150-300ms and clear it. If it doesn't find one
  // Electiion

  def poll: UIO[Message] = d.outboundQueue.take.forever.commit

  def offerVote(vote: VoteResponse) = d.inboundVoteResponseQueue.offer(vote).commit

  private[raft] def sendMessage(m: Message): USTM[Unit] = d.outboundQueue.offer(m).unit

  private[raft] def sendMessages(ms: Iterable[Message]): USTM[Unit] = d.outboundQueue.offerAll(ms).unit

  private[raft] def becomeCandidate: USTM[Unit] = d.raftNode.update(_.stand)

  private[raft] def nodeState: UIO[NodeState] = d.raftNode.get.commit.map(_.state)

  private[raft] def nodeId: USTM[RaftNode.Id] = d.raftNode.map(_.id).get

  private[raft] def isLeader: UIO[Boolean] = d.raftNode.get.commit.map(_.isLeader)

  private def sendHeartbeats = {
    val hearbeatProgram = for {
      currentNode <- d.raftNode.get
      heartbeats = currentNode.peers.map(p => Heartbeat(currentNode.id, p, currentNode.term))
      _ <- sendMessages(heartbeats)
    } yield ()
    
    hearbeatProgram
      .commit
      .repeatUntilM(_ => isLeader.map(!_))
      .repeat(Schedule.spaced(Raft.LeaderHeartbeat))
  }

  private def sendVoteRequests: USTM[Unit] = for {
    currentNode <- d.raftNode.get
    peers = currentNode.peers
    newTerm = currentNode.term.increment
    requests = peers.map(p => VoteRequest(currentNode.id, p, newTerm))
    _ <- sendMessages(requests)
  } yield ()

  private def collectVotes: UIO[Unit] = {
    lazy val voteCollectionProgram = for {
      maybeNewVote <- d.inboundVoteResponseQueue.poll
      currentNode <- d.raftNode.get
      _ <- maybeNewVote.fold(ZSTM.unit) { newVote =>
        d.raftNode.set(
          currentNode.addVote(newVote.from, newVote.term)
        )    
      }
    } yield ()

    voteCollectionProgram
      .commit
      .repeatUntilM(_ => 
        d.raftNode.get.commit.map(_.state != NodeState.Candidate)
      )
      .unit
  }

  def runForLeader = (becomeCandidate *> sendVoteRequests).commit *> collectVotes

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
  
  def run = {
    lazy val mainProgram = for {
      _ <- runFollowerLoop
    } yield ()
   
    mainProgram.forever.unit
  }

}

object Raft {

  case class Dependencies(
    raftNode: TRef[RaftNode],
    inboundVoteResponseQueue: TQueue[VoteResponse],
    inboundVoteRequestQueue: TQueue[VoteRequest],
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
      heartbeatQueue <- newQueue[Heartbeat](queueSize)
      outboundQueue <- newQueue[Message](queueSize)
    } yield Dependencies(
      node,
      inboundVoteResponseQueue,
      inboundVoteRequestQueue,
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