package com.ariskk.raft.storage

import zio.stm._
import zio.UIO

import com.ariskk.raft.storage.Storage
import com.ariskk.raft.model._

/**
 * Reference implemantation of `Storage` for testing purposes.
 */
final class MemoryStorage[T](
  private[raft] listRef: TRef[List[LogEntry[T]]],
  private[raft] votedForRef: TRef[Option[Vote]],
  private[raft] termRef: TRef[Term]
) extends Storage[T] {
  lazy val log = new MemoryLog(listRef)

  def storeVote(vote: Vote): STM[StorageException, Unit] = votedForRef.set(Option(vote))

  def getVote: STM[StorageException, Option[Vote]] = votedForRef.get

  def storeTerm(term: Term): STM[StorageException, Unit] = termRef.set(term)

  def getTerm: STM[StorageException, Term] = termRef.get

}

final class MemoryLog[T](log: TRef[List[LogEntry[T]]]) extends Log[T] {
  def append(entry: LogEntry[T]): STM[StorageException, Unit] = log.update(_ :+ entry)
  def size: STM[StorageException, Long]                       = log.get.map(_.size.toLong)
  def getEntry(index: Index): STM[StorageException, Option[LogEntry[T]]] =
    log.get.map(_.lift(index.index.toInt))
  def getEntries(fromIndex: Index): STM[StorageException, List[LogEntry[T]]] =
    log.get.map(_.drop(fromIndex.index.toInt))
  def purgeFrom(index: Index): STM[StorageException, Unit] = log.get.map(l => l.dropRight(l.size - index.index.toInt))
}

object MemoryStorage {

  def default[T]: UIO[MemoryStorage[T]] = for {
    termRef     <- TRef.makeCommit(Term.Zero)
    votedForRef <- TRef.makeCommit(Option.empty[Vote])
    log         <- TRef.makeCommit(List.empty[LogEntry[T]])
  } yield new MemoryStorage(log, votedForRef, termRef)

}
