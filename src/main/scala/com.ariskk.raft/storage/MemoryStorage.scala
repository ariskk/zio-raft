package com.ariskk.raft.storage

import zio.stm._
import zio.UIO

import com.ariskk.raft.storage.Storage
import com.ariskk.raft.model._

/**
 * Reference implemantation of `Storage` for testing purposes.
 */
final class MemoryStorage[T](
  private[raft] log: TQueue[LogEntry[T]],
  private[raft] votedForRef: TRef[Option[Vote]],
  private[raft] termRef: TRef[Term]
) extends Storage[T] {

  def appendEntry(entry: LogEntry[T]): STM[StorageException, Unit] = log.offer(entry)

  def logSize: STM[StorageException, Long] = log.size.map(_.toLong)

  def storeVote(vote: Vote): STM[StorageException, Unit] = votedForRef.set(Option(vote))

  def getVote: STM[StorageException, Option[Vote]] = votedForRef.get

  def storeTerm(term: Term): STM[StorageException, Unit] = termRef.set(term)

  def getTerm: STM[StorageException, Term] = termRef.get

}

object MemoryStorage {

  def default[T]: UIO[MemoryStorage[T]] = for {
    termRef     <- TRef.makeCommit(Term.Zero)
    votedForRef <- TRef.makeCommit(Option.empty[Vote])
    log         <- TQueue.unbounded[LogEntry[T]].commit
  } yield new MemoryStorage(log, votedForRef, termRef)

}
