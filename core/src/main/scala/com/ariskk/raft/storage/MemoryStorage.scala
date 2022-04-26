package com.ariskk.raft.storage

import com.ariskk.raft.model.RaftException.StorageException
import com.ariskk.raft.model._
import zio.{ UIO, _ }

/**
 * Reference implemantation of `Storage` for testing purposes.
 */
final class MemoryStorage(
  private[raft] val listRef: Ref[List[LogEntry]],
  private[raft] val votedForRef: Ref[Option[Vote]],
  private[raft] val termRef: Ref[Term]
) extends Storage {
  lazy val log = new MemoryLog(listRef)

  def storeVote(vote: Vote): IO[StorageException, Unit] = votedForRef.set(Option(vote))

  def getVote: IO[StorageException, Option[Vote]] = votedForRef.get

  def storeTerm(term: Term): IO[StorageException, Unit] = termRef.set(term)

  def getTerm: IO[StorageException, Term] = termRef.get

}

final class MemoryLog(log: Ref[List[LogEntry]]) extends Log {
  def append(index: Index, entry: LogEntry): IO[StorageException, Unit] =
    log.update(_.patch(index.index.toInt, Seq(entry), 1))
  def size: IO[StorageException, Long] = log.get.map(_.size.toLong)
  def getEntry(index: Index): IO[StorageException, Option[LogEntry]] =
    log.get.map(_.lift(index.index.toInt))
  def getEntries(fromIndex: Index): IO[StorageException, List[LogEntry]] =
    log.get.map(_.drop(fromIndex.index.toInt))
  def purgeFrom(index: Index): IO[StorageException, Unit] = log.get.map(l => l.dropRight(l.size - index.index.toInt))
  def getRange(from: Index, to: Index) =
    log.get.map(_.slice(from.index.toInt, to.increment.index.toInt))
}

object MemoryStorage {

  def default: UIO[MemoryStorage] = for {
    termRef     <- Ref.make(Term.Zero)
    votedForRef <- Ref.make(Option.empty[Vote])
    log         <- Ref.make(List.empty[LogEntry])
  } yield new MemoryStorage(log, votedForRef, termRef)

}
