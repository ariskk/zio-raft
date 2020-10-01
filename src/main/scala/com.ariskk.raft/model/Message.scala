package com.ariskk.raft.model

import com.ariskk.raft.utils.Utils

sealed trait Message {
  def from: NodeId
  def to: NodeId
  def term: Term
}

object Message {

  final case class VoteRequest(
    from: NodeId,
    to: NodeId,
    term: Term
  ) extends Message

  final case class VoteResponse(
    from: NodeId,
    to: NodeId,
    term: Term,
    granted: Boolean
  ) extends Message

  object AppendEntries {
    case class Id(value: String) extends AnyVal

    def newUniqueId = Id(Utils.newPrefixedId("append"))
  }

  final case class AppendEntries[T](
    appendId: AppendEntries.Id,
    from: NodeId,
    to: NodeId,
    term: Term,
    prevLogIndex: Index,
    prevLogTerm: Term,
    leaderCommitIndex: Index,
    entries: Seq[T]
  ) extends Message

  final case class AppendEntriesResponse(
    from: NodeId,
    to: NodeId,
    appendId: AppendEntries.Id,
    term: Term,
    success: Boolean
  ) extends Message

}
