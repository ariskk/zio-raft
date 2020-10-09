package com.ariskk.raft.storage

import zio.stm._

import com.ariskk.raft.model._

trait Log {
  def append(entry: LogEntry): STM[StorageException, Unit]
  def size: STM[StorageException, Long]
  def getEntry(index: Index): STM[StorageException, Option[LogEntry]]
  def getEntries(index: Index): STM[StorageException, List[LogEntry]]
  def purgeFrom(index: Index): STM[StorageException, Unit]
  def getRange(from: Index, to: Index): STM[StorageException, List[LogEntry]]
}
