package com.ariskk.raft.model

final case class Term(term: Long) extends AnyVal {
  def increment: Term      = this.copy(term + 1)
  def isAfter(other: Term) = term > other.term
}

object Term {
  lazy val Zero    = Term(0)
  lazy val Invalid = Term(Long.MinValue)
}
