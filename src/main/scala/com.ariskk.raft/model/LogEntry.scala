package com.ariskk.raft.model

import com.ariskk.raft.utils.Utils

case class LogEntry[T](
  id: LogEntry.Id,
  command: Command[T],
  term: Term
)

object LogEntry {
  case class Id(value: String) extends AnyVal

  def newUniqueId = Id(Utils.newPrefixedId("entry"))

  def apply[T](command: Command[T], term: Term): LogEntry[T] = LogEntry[T](
    newUniqueId,
    command,
    term
  )
}
