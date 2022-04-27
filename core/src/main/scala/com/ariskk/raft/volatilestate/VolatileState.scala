package com.ariskk.raft.volatilestate

import zio.{ UIO, _ }

import com.ariskk.raft.model._

final class VolatileState(
  val nodeId: NodeId,
  peers: Ref[Set[NodeId]],
  currentLeader: Ref[Option[NodeId]],
  state: Ref[NodeState],
  votesReceived: Ref[Set[Vote]],
  votesRejected: Ref[Set[Vote]],
  commitIndex: Ref[Index],
  lastApplied: Ref[Index],
  nextIndex: Ref[Map[NodeId, Index]],
  matchIndex: Ref[Map[NodeId, Index]]
) {
  def stand(newTerm: Term) = for {
    _ <- votesReceived.update(_.filterNot(_.term != newTerm))
    _ <- votesRejected.update(_.filterNot(_.term != newTerm))
    _ <- state.set(NodeState.Candidate)
    _ <- addVote(Vote(nodeId, newTerm))
  } yield ()

  def peerList = peers.get.map(_.toList)

  def nextIndexForPeer(peerId: NodeId) = nextIndex.get.map(_.get(peerId))

  def matchIndexForPeer(peerId: NodeId) = matchIndex.get.map(_.get(peerId))

  def updateMatchIndex(peerId: NodeId, index: Index) =
    matchIndex.update(_ + (peerId -> index))

  def updateCommitIndex(index: Index) = commitIndex.set(index)

  def updateNextIndex(peerId: NodeId, index: Index) =
    nextIndex.update(_ + (peerId -> index))

  def decrementNextIndex(peerId: NodeId) = for {
    next <- nextIndexForPeer(peerId)
    nextIndex = next.map(x => if (x == Index(0)) x else x.decrement).getOrElse(Index(0))
    _ <- updateNextIndex(peerId, nextIndex)
  } yield ()

  def matchIndexEntries = matchIndex.get.map(_.toList)

  def initPeerIndices(lastIndex: Index) = for {
    peers <- peerList
    _     <- ZIO.collectAll(peers.map(p => nextIndex.update(_ + (p -> lastIndex.increment))))
    _     <- ZIO.collectAll(peers.map(p => matchIndex.update(_ + (p -> Index(0)))))
  } yield ()

  def setLastApplied(index: Index) = lastApplied.set(index)

  def incrementLastApplied = lastApplied.update(_.increment)

  def nodeState = state.get

  def addPeer(id: NodeId) = peers.update(_ + id)

  def removePeer(id: NodeId) = peers.update(_ - id)

  def becomeFollower = state.set(NodeState.Follower)

  def becomeLeader = for {
    _ <- state.set(NodeState.Leader)
    _ <- setLeader(nodeId)
  } yield ()

  def setLeader(leaderId: NodeId) = currentLeader.set(Option(leaderId))

  def leader = currentLeader.get

  def addVote(vote: Vote) = for {
    _     <- votesReceived.update(_.filter(_.term == vote.term))
    _     <- votesReceived.update(_ + vote)
    set   <- votesReceived.get.map(_.toList)
    peers <- peerList
    hasMajority = 2 * set.size > peers.size + 1
    _ <- ZIO.when(hasMajority)(becomeLeader)
  } yield hasMajority

  def addVoteRejection(vote: Vote) = for {
    _     <- votesRejected.update(_.filter(_.term == vote.term))
    _     <- votesRejected.update(_ + vote)
    set   <- votesRejected.get
    peers <- peerList
    hasLost = 2 * set.size > peers.size + 1
    _ <- ZIO.when(hasLost)(becomeFollower)
  } yield hasLost

  def hasLost(term: Term) = for {
    vr    <- votesRejected.get.map(_.toList)
    peers <- peerList
    rejections = vr.filter(_.term == term)
  } yield 2 * rejections.size > peers.size + 1

  def isLeader    = state.map(_ == NodeState.Leader).get
  def isFollower  = state.map(_ == NodeState.Follower).get
  def isCandidate = state.map(_ == NodeState.Candidate).get

  def lastCommitIndex = commitIndex.get

}

object VolatileState {
  def apply(nodeId: NodeId, peers: Set[NodeId]): UIO[VolatileState] = for {
    peerRef          <- Ref.make[Set[NodeId]](peers)
    leaderRef        <- Ref.make[Option[NodeId]](None)
    stateRef         <- Ref.make[NodeState](NodeState.Follower)
    votesReceivedRef <- Ref.make(Set.empty[Vote])
    votesRejectedRef <- Ref.make(Set.empty[Vote])
    commitIndex      <- Ref.make(Index(-1L))
    lastApplied      <- Ref.make(Index(-1L))
    nextIndex        <- Ref.make(peers.map(p => (p, Index(0))).toMap)
    matchIndex       <- Ref.make(peers.map(p => (p, Index(-1))).toMap)
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
