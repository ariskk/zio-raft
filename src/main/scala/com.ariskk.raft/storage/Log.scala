package com.ariskk.raft.storage

import zio.stm._

import com.ariskk.raft.model._

trait Log[T] {
  def append(entry: LogEntry[T]): STM[StorageException, Unit]
  def size: STM[StorageException, Long]
  def getEntry(index: Index): STM[StorageException, Option[LogEntry[T]]]
  def getEntries(index: Index): STM[StorageException, List[LogEntry[T]]]
}
