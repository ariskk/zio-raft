package com.ariskk.raft.model

case class LogEntry[T](
  commmand: T,
  term: Term
)
