package com.ariskk.raft.state

import zio.stm._
import zio.UIO

import com.ariskk.raft.model._

final class VolatileState(
  val nodeId: NodeId,
  peers: TSet[NodeId],
  currentLeader: TRef[Option[NodeId]],
  state: TRef[NodeState],
  votesReceived: TSet[Vote],
  votesRejected: TSet[Vote],
  commitIndex: TRef[Index],
  lastApplied: TRef[Index],
  nextIndex: TMap[NodeId, Index],
  matchIndex: TMap[NodeId, Index]
) {
  def stand(newTerm: Term) = for {
    _ <- votesReceived.removeIf(_.term != newTerm)
    _ <- votesRejected.removeIf(_.term != newTerm)
    _ <- state.set(NodeState.Candidate)
    _ <- addVote(Vote(nodeId, newTerm))
  } yield ()

  def peerList = peers.toList

  def nextIndexForPeer(peerId: NodeId) = nextIndex.get(peerId)

  def matchIndexForPeer(peerId: NodeId) = matchIndex.get(peerId)

  def updateMatchIndex(peerId: NodeId, index: Index) =
    matchIndex.put(peerId, index)

  def updateCommitIndex(index: Index) = commitIndex.set(index)

  def updateNextIndex(peerId: NodeId, index: Index) =
    nextIndex.put(peerId, index)

  def decrementNextIndex(peerId: NodeId) = for {
    next <- nextIndexForPeer(peerId)
    nextIndex = next.map(x => if (x == Index(0)) x else x.decrement).getOrElse(Index(0))
    _ <- updateNextIndex(peerId, nextIndex)
  } yield ()

  def matchIndexEntries = matchIndex.toList

  def nodeState = state.get

  def addPeer(id: NodeId) = peers.put(id)

  def removePeer(id: NodeId) = peers.delete(id)

  def becomeFollower = state.set(NodeState.Follower)

  def becomeLeader = for {
    _ <- state.set(NodeState.Leader)
    _ <- setLeader(nodeId)
    // todo need to set the indices here
  } yield ()

  def setLeader(leaderId: NodeId) = currentLeader.set(Option(leaderId))

  def leader = currentLeader.get

  def addVote(vote: Vote) = for {
    _     <- votesReceived.retainIf(_.term == vote.term)
    _     <- votesReceived.put(vote)
    set   <- votesReceived.toList
    peers <- peers.toList
    hasMajority = 2 * set.size > peers.size + 1
    _ <- if (hasMajority) becomeLeader else ZSTM.unit
  } yield ()

  def addVoteRejection(vote: Vote) = for {
    _     <- votesRejected.retainIf(_.term == vote.term)
    _     <- votesRejected.put(vote)
    set   <- votesRejected.toList
    peers <- peers.toList
    hasLost = 2 * set.size > peers.size + 1
    _ <- if (hasLost) becomeFollower else ZSTM.unit
  } yield ()

  def hasLost(term: Term) = for {
    vr    <- votesRejected.toList
    peers <- peerList
    rejections = vr.filter(_.term == term)
  } yield 2 * rejections.size > peers.size + 1

  def isLeader    = state.map(_ == NodeState.Leader).get
  def isFollower  = state.map(_ == NodeState.Follower).get
  def isCandidate = state.map(_ == NodeState.Candidate).get

  def lastCommitIndex = commitIndex.get

}

object VolatileState {
  def apply[T](nodeId: NodeId, peers: Set[NodeId]): UIO[VolatileState] = for {
    peerRef          <- TSet.make[NodeId](peers.toSeq: _*).commit
    leaderRef        <- TRef.makeCommit[Option[NodeId]](None)
    stateRef         <- TRef.makeCommit[NodeState](NodeState.Follower)
    votesReceivedRef <- TSet.empty[Vote].commit
    votesRejectedRef <- TSet.empty[Vote].commit
    commitIndex      <- TRef.makeCommit(Index(-1L))
    lastApplied      <- TRef.makeCommit(Index(-1L))
    nextIndex        <- TMap.fromIterable(peers.map(p => (p, Index(0)))).commit
    matchIndex       <- TMap.fromIterable(peers.map(p => (p, Index(-1)))).commit
  } yield new VolatileState(
    nodeId,
    peerRef,
    leaderRef,
    stateRef,
    votesReceivedRef,
    votesRejectedRef,
    commitIndex,
    lastApplied,
    nextIndex,
    matchIndex
  )
}
