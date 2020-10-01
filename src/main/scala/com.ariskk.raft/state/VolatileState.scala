package com.ariskk.raft.state

import zio.stm._
import zio.UIO

import com.ariskk.raft.model._

final class VolatileState(
  val nodeId: NodeId,
  peers: TSet[NodeId],
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
    _ <- addVote(Vote(nodeId, newTerm))
  } yield ()

  def peerList = peers.toList

  def nodeState = state.get

  def addPeer(id: NodeId) = peers.put(id)

  def removePeer(id: NodeId) = peers.delete(id)

  def becomeFollower = for {
    _     <- state.set(NodeState.Follower)
    empty <- TSet.empty[Vote]
    _     <- votesReceived.intersect(empty)
    _     <- votesRejected.intersect(empty)
  } yield ()

  def becomeLeader = state.set(NodeState.Leader)

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
    vr          <- votesRejected.toList
    peers       <- peerList
    rejections = vr.filter(_.term == term)
  } yield 2 * rejections.size > peers.size + 1

  def isLeader    = state.map(_ == NodeState.Leader).get
  def isFollower  = state.map(_ == NodeState.Follower).get
  def isCandidate = state.map(_ == NodeState.Candidate).get

}

object VolatileState {
  def apply[T](nodeId: NodeId, peers: Set[NodeId]): UIO[VolatileState] = for {
    peerRef          <- TSet.make[NodeId](peers.toSeq: _*).commit
    stateRef         <- TRef.makeCommit[NodeState](NodeState.Follower)
    votesReceivedRef <- TSet.empty[Vote].commit
    votesRejectedRef <- TSet.empty[Vote].commit
    commitIndex      <- TRef.makeCommit(Index(-1L))
    lastApplied      <- TRef.makeCommit(Index(-1L))
    nextIndex        <- TMap.empty[NodeId, Index].commit
    matchIndex       <- TMap.empty[NodeId, Index].commit
  } yield new VolatileState(
    nodeId,
    peerRef,
    stateRef,
    votesReceivedRef,
    votesRejectedRef,
    commitIndex,
    lastApplied,
    nextIndex,
    matchIndex
  )
}
