package com.ariskk.raft.storage

import zio._

import com.ariskk.raft.model.RaftException.StorageException
import com.ariskk.raft.model._

/**
 * Interface of persistent server state.
 * Current term, latest node vote and all log entries must be committed here
 * to stable storage before responding to RPC.
 * For more info, look at Figure 2 of https://raft.github.io/raft.pdf
 */
trait Storage {
  def log: Log

  def appendEntry(index: Index, entry: LogEntry): IO[StorageException, Unit] = log.append(index, entry)

  def appendEntries(fromIndex: Index, entries: List[LogEntry]): IO[StorageException, Unit] = {
    val indicies = (fromIndex.index to fromIndex.index + entries.size - 1).toList
    ZIO
      .collectAll(
        indicies.zip(entries).map { case (i, e) => appendEntry(Index(i), e) }
      )
      .unit
  }

  def getEntry(index: Index): IO[StorageException, Option[LogEntry]] = log.getEntry(index)

  def getEntries(fromIndex: Index): IO[StorageException, List[LogEntry]] =
    log.getEntries(fromIndex)

  def getRange(from: Index, to: Index): IO[StorageException, List[LogEntry]] =
    log.getRange(from, to)

  def lastEntry: IO[StorageException, Option[LogEntry]] = for {
    size <- logSize
    last <- getEntry(Index(size - 1))
  } yield last

  def lastIndex: IO[StorageException, Index] = logSize.map(s => Index(s - 1))

  def purgeFrom(index: Index): IO[StorageException, Unit] = log.purgeFrom(index)

  def logSize: IO[StorageException, Long] = log.size

  def storeVote(vote: Vote): IO[StorageException, Unit]

  def getVote: IO[StorageException, Option[Vote]]

  def storeTerm(term: Term): IO[StorageException, Unit]

  def getTerm: IO[StorageException, Term]

  def incrementTerm: IO[StorageException, Term] = for {
    term    <- getTerm
    newTerm <- ZIO.succeed(term.increment)
    _       <- storeTerm(newTerm)
  } yield newTerm

}
