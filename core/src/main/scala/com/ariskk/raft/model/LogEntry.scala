package com.ariskk.raft.model

import com.ariskk.raft.model.Command.WriteCommand
import com.ariskk.raft.utils.Utils

case class LogEntry(
  id: LogEntry.Id,
  command: WriteCommand,
  term: Term
)

object LogEntry {
  case class Id(value: String) extends AnyVal

  def newUniqueId: Id = Id(Utils.newPrefixedId("entry"))

  def apply(command: WriteCommand, term: Term): LogEntry = LogEntry(
    newUniqueId,
    command,
    term
  )
}
