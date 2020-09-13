package com.ariskk.raft.model

// TODO Model fron/to better, those are easy to mess up
sealed trait Message {
  def from: RaftNode.Id
  def to: RaftNode.Id
  def term: Term
}

object Message {

  final case class VoteRequest(
    from: RaftNode.Id,
    to: RaftNode.Id,
    term: Term
  ) extends Message

  final case class VoteResponse(
    from: RaftNode.Id,
    to: RaftNode.Id,
    term: Term,
    granted: Boolean
  ) extends Message

  final case class Heartbeat(
    from: RaftNode.Id,
    to: RaftNode.Id,
    term: Term
  ) extends Message

  final case class HeartbeatAck(
    rom: RaftNode.Id,
    to: RaftNode.Id,
    term: Term
  )

  final case class AppendEntries(
    from: RaftNode.Id,
    to: RaftNode.Id,
    term: Term
  ) extends Message

}

