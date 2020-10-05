package com.ariskk.raft.model

case class Index(index: Long) extends AnyVal {
  def decrement            = Index(index - 1L)
  def increment            = Index(index + 1)
  def >(otherIndex: Index) = index > otherIndex.index
}
