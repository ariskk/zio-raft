package com.ariskk.raft.rocksdb

import org.rocksdb.RocksDB
import zio._

import com.ariskk.raft.storage.Log
import com.ariskk.raft.model._

/**
 * The current index is kept in a special key called `index`.
 */
final class RocksDBLog(
  db: Ref[RocksDB],
  serdeRef: Ref[Serde]
) extends RocksDBSupport(db, serdeRef)
    with Log {

  private val LogIndex = "index".getBytes()
  private val Start    = -1

  private[raft] def logEntryKey(index: Int) = s"logEntry-${index}".getBytes()

  private[raft] def lastIndex: IO[StorageException, Int] = getKey[Int](LogIndex)
    .map(_.getOrElse(Start))

  private[raft] def incrementIndex: IO[StorageException, Int] = for {
    current <- lastIndex
    incremented = current + 1
    _ <- putKey(LogIndex, incremented)
  } yield incremented

  def append(index: Index, entry: LogEntry): IO[StorageException, Unit] =
    putKey(LogIndex, index.index.toInt) *> putKey(logEntryKey(index.index.toInt), entry)

  def size: IO[StorageException, Long] = lastIndex.map(index => (index + 1).toLong)

  def getEntry(index: Index): IO[StorageException, Option[LogEntry]] =
    getKey[LogEntry](logEntryKey(index.index.toInt))

  def getEntries(index: Index): IO[StorageException, List[LogEntry]] =
    lastIndex.flatMap(l => getKeys[LogEntry]((index.index to l).map(i => logEntryKey(i.toInt)).toList))

  def purgeFrom(index: Index): IO[StorageException, Unit] = for {
    last  <- lastIndex
    serde <- serdeRef.get
    _     <- deleteKeys((index.index to last).map(i => serde.serialize(logEntryKey(i.toInt))).toList)
    _     <- putKey[Int](LogIndex, index.decrement.index.toInt)
  } yield ()

  def getRange(from: Index, to: Index): IO[StorageException, List[LogEntry]] =
    getKeys[LogEntry]((from.index to to.index).map(i => logEntryKey(i.toInt)).toList)

}
