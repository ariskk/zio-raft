package com.ariskk.raft.model

import java.util.UUID

final case class RaftNode(
  id: RaftNode.Id,
  term: Term,
  peers: Set[RaftNode.Id],
  votedFor: Option[RaftNode.Id],
  state: NodeState,
  leader: Option[RaftNode.Id],
  votesReceived: Set[Vote],
  votesRejected: Set[Vote]
) {
  lazy val stand = {
    val newTerm = term.increment
    this.copy(
      term = newTerm,
      state = NodeState.Candidate
    ).addVote(id, newTerm).voteFor(id)
  }

  def addPeer(peer: RaftNode.Id) = this.copy(
    peers = peers + peer
  )

  def removePeer(peer: RaftNode.Id) = this.copy(
    peers = peers - peer
  )

  def becomeFollower(newTerm: Term) = this.copy(
    term = newTerm,
    state = NodeState.Follower
  ).clearVotes(newTerm).clearVoteRejections(newTerm)

  def becomeLeaeder = this.copy(
    state = NodeState.Leader
  )

  def addVote(voter: RaftNode.Id, term: Term) = {
    val currentTermVotes = votesReceived.filter(_.term == term)
    val updatedVotes = currentTermVotes + Vote(voter, term)
    val hasMajority = 2 * updatedVotes.size > peers.size + 1
    if (hasMajority)
      this.copy(votesReceived = updatedVotes).becomeLeaeder
    else this.copy(votesReceived = updatedVotes)
  }

  def clearVotes(term: Term) = this.copy(
    votesReceived = votesReceived.filterNot(_.term.term < term.term)
  )

  def voteFor(peer: RaftNode.Id) = this.copy(
    votedFor = Option(peer)
  )

  def addVoteRejection(voter: RaftNode.Id, term: Term) = {
    val currentRejections = votesRejected.filter(_.term == term)
    val updated = currentRejections + Vote(voter, term)
    val hasLost = 2 * updated.size > peers.size + 1
    if (hasLost) 
      this.copy(votesRejected = updated).becomeFollower(term)
    else  this.copy(votesRejected = updated)
  }
  

  def clearVoteRejections(term: Term) = this.copy(
    votesRejected = votesRejected.filterNot(_.term.term < term.term)
  )

  def hasLost(term: Term) = {
    val rejections = votesRejected.filter(_.term == term)
    2 * rejections.size > peers.size + 1
  }

  lazy val isLeader: Boolean = state == NodeState.Leader

  lazy val isCandidate: Boolean = state == NodeState.Candidate

  lazy val isFollower: Boolean = state == NodeState.Follower

}

object RaftNode {
  final case class Id(value: String) extends AnyVal

  private[raft] def newUniqueId: Id = {
    val value = s"node-${UUID.randomUUID().toString.take(20)}"
    Id(value)
  }

  def initial(nodeId: Id, peers: Set[Id]) = RaftNode(
    nodeId,
    Term.Zero,
    peers = peers,
    votedFor = None,
    NodeState.default,
    leader = None,
    votesReceived = Set.empty,
    votesRejected = Set.empty
  )
}
