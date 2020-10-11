package com.ariskk.raft.storage

import zio.stm._

import com.ariskk.raft.model._

/**
 * Interface of persistent server state.
 * Current term, latest node vote and all log entries must be committed here
 * to stable storage before responding to RPC.
 * For more info, look at Figure 2 of https://raft.github.io/raft.pdf
 */
trait Storage {
  def log: Log

  def appendEntry(entry: LogEntry): STM[StorageException, Unit] = log.append(entry)

  def appendEntries(entries: List[LogEntry]): STM[StorageException, Unit] = ZSTM
    .collectAll(
      entries.map(appendEntry)
    )
    .unit

  def getEntry(index: Index): STM[StorageException, Option[LogEntry]] = log.getEntry(index)

  def getEntries(fromIndex: Index): STM[StorageException, List[LogEntry]] =
    log.getEntries(fromIndex)

  def getRange(from: Index, to: Index): STM[StorageException, List[LogEntry]] =
    log.getRange(from, to)

  def lastEntry: STM[StorageException, Option[LogEntry]] = for {
    size <- logSize
    last <- getEntry(Index(size - 1))
  } yield last

  def lastIndex: STM[StorageException, Index] = logSize.map(s => Index(s - 1))

  def purgeFrom(index: Index): STM[StorageException, Unit] = log.purgeFrom(index)

  def logSize: STM[StorageException, Long] = log.size

  def storeVote(vote: Vote): STM[StorageException, Unit]

  def getVote: STM[StorageException, Option[Vote]]

  def storeTerm(term: Term): STM[StorageException, Unit]

  def getTerm: STM[StorageException, Term]

}
