package com.ariskk.raft.rocksdb

import java.io._
import java.nio.file.Files

import org.rocksdb._
import zio._

import com.ariskk.raft.model.RaftException.StorageException
import com.ariskk.raft.model._
import com.ariskk.raft.storage.Storage

final class RocksDBStorage(
  dbRef: Ref[RocksDB],
  serdeRef: Ref[Serde]
) extends RocksDBSupport(dbRef, serdeRef)
    with Storage {

  private val VoteKey = "vote".getBytes()
  private val TermKey = "term".getBytes()

  lazy val log = new RocksDBLog(dbRef, serdeRef)

  def getTerm: IO[StorageException, Term]               = getKey[Term](TermKey).map(_.getOrElse(Term.Zero))
  def getVote: IO[StorageException, Option[Vote]]       = getKey[Vote](VoteKey)
  def storeTerm(term: Term): IO[StorageException, Unit] = putKey[Term](TermKey, term)
  def storeVote(vote: Vote): IO[StorageException, Unit] = putKey[Vote](VoteKey, vote)

}

object RocksDBStorage {
  def apply(dbDirectory: String, dbName: String): ZIO[Any, Throwable, RocksDBStorage] = {
    val options = new Options()
    options.setCreateIfMissing(true)
    val dbDir = new File(dbDirectory, dbName)

    for {
      _        <- ZIO.effect(RocksDB.loadLibrary())
      _        <- ZIO.effect(Files.createDirectories(dbDir.getParentFile.toPath))
      _        <- ZIO.effect(Files.createDirectories(dbDir.getAbsoluteFile.toPath))
      db       <- ZIO.effect(RocksDB.open(options, dbDir.getAbsolutePath))
      dbRef    <- Ref.make(db)
      serdeRef <- Ref.make(Serde.kryo)
    } yield new RocksDBStorage(dbRef, serdeRef)

  }
}
