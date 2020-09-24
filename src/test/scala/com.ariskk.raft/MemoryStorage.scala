package com.ariskk.raft

import zio.stm._
import zio.UIO

import com.ariskk.raft.storage.Storage
import com.ariskk.raft.model._

final class MemoryStorage[T](
  private[raft] log: TQueue[LogEntry[T]],
  private[raft] votedForRef: TRef[Option[RaftNode.Id]],
  private[raft] termRef: TRef[Term]
) extends Storage[T] {

  def appendEntry(entry: LogEntry[T]): STM[StorageException, Unit] = log.offer(entry)

  def logSize: STM[StorageException, Long] = log.size.map(_.toLong)

  def storeVote(node: RaftNode.Id): STM[StorageException, Unit] = votedForRef.set(Option(node))

  def getVote: STM[StorageException, Option[RaftNode.Id]] = votedForRef.get

  def storeTerm(term: Term): STM[StorageException, Unit] = termRef.set(term)

  def getTerm: STM[StorageException, Term] = termRef.get

}

object MemoryStorage {

  def default[T]: UIO[MemoryStorage[T]] = for {
    termRef     <- TRef.makeCommit(Term.Zero)
    votedForRef <- TRef.makeCommit(Option.empty[RaftNode.Id])
    log         <- TQueue.unbounded[LogEntry[T]].commit
  } yield new MemoryStorage(log, votedForRef, termRef)

}
