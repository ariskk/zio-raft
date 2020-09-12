package com.ariskk.raft.model

// TODO Model fron/to better, those are easy to mess up
sealed trait Message {
  def from: RaftNode.Id
  def to: RaftNode.Id
  def term: Term
}

object Message {

  // this is shit modeling. Improve
  trait VoteMessage

  final case class VoteRequest(
    from: RaftNode.Id,
    to: RaftNode.Id,
    term: Term
  ) extends Message with VoteMessage

  final case class VoteResponse(
    from: RaftNode.Id,
    to: RaftNode.Id,
    term: Term,
    granted: Boolean
  ) extends Message with VoteMessage

  trait Append

  final case class Heartbeat(
    from: RaftNode.Id,
    to: RaftNode.Id,
    term: Term
  ) extends Message with Append

  final case class AppendEntries(
    from: RaftNode.Id,
    to: RaftNode.Id,
    term: Term
  ) extends Message with Append

}

