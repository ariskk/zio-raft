package com.ariskk.raft.model

import com.ariskk.raft.utils.Utils

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

  object AppendEntries {
    case class Id(value: String) extends AnyVal

    def newUniqueId = Id(Utils.newPrefixedId("append"))
  }

  final case class AppendEntries[T](
    appendId: AppendEntries.Id,
    from: RaftNode.Id,
    to: RaftNode.Id,
    term: Term,
    prevLogIndex: Index,
    prevLogTerm: Term,
    leaderCommitIndex: Index,
    entries: Seq[T]
  ) extends Message

  final case class AppendEntriesResponse(
    from: RaftNode.Id,
    to: RaftNode.Id,
    appendId: AppendEntries.Id,
    term: Term,
    success: Boolean
  ) extends Message

}
