package com.ariskk.raft.storage

import com.ariskk.raft.model.RaftException.StorageException
import zio._
import com.ariskk.raft.model._

trait Log {
  def append(index: Index, entry: LogEntry): IO[StorageException, Unit]
  def size: IO[StorageException, Long]
  def getEntry(index: Index): IO[StorageException, Option[LogEntry]]
  def getEntries(index: Index): IO[StorageException, List[LogEntry]]
  def purgeFrom(index: Index): IO[StorageException, Unit]
  def getRange(from: Index, to: Index): IO[StorageException, List[LogEntry]]
}
