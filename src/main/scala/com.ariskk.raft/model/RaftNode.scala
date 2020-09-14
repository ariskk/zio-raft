package com.ariskk.raft.model

import java.util.UUID

final case class RaftNode(
  id: RaftNode.Id,
  term: Term,
  peers: Set[RaftNode.Id],
  votedFor: Option[RaftNode.Id],
  state: NodeState,
  leader: Option[RaftNode.Id],
  votesReceived: Set[Vote]
) {
  lazy val stand = {
    val newTerm = term.increment
    this.copy(
      term = newTerm,
      state = NodeState.Candidate
    ).addVote(id, newTerm)
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
  ).clearVotes

  def becomeLeaeder = this.copy(
    state = NodeState.Leader
  )

  def addVote(voter: RaftNode.Id, term: Term) = {
    val updatedVotes = votesReceived + Vote(voter, term)
    val hasMajority = updatedVotes.size > ((peers.size + 1) / 2)
    if (hasMajority)
      this.copy(votesReceived = updatedVotes).becomeLeaeder
    else this.copy(votesReceived = updatedVotes)
  }

  def clearVotes = this.copy(
    votesReceived = Set.empty[Vote]
  )

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

  def initial(
    nodeId: Id = newUniqueId, 
    peers: Set[Id] = Set.empty
  ) = RaftNode(
    newUniqueId,
    Term.Zero,
    peers = peers,
    votedFor = None,
    NodeState.default,
    leader = None,
    votesReceived = Set.empty
  )
}
