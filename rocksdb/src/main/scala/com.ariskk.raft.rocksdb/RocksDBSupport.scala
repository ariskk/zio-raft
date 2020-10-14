package com.ariskk.raft.rocksdb

import scala.util.Try
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

import zio._
import org.rocksdb.RocksDB

import com.ariskk.raft.model._

abstract class RocksDBSupport(
  dbRef: Ref[RocksDB],
  serdeRef: Ref[Serde]
) {

  private def withDB[T, R](f: RocksDB => T) = for {
    rocksDB <- dbRef.get
    bytes <- ZIO
      .fromTry(Try(f(rocksDB)))
      .mapError(e => StorageException("Command to RocksDB failed", Option(e)))
  } yield bytes

  private def deserialize[T: ClassTag](bytes: Array[Byte]) =
    serdeRef.get.flatMap(s => ZIO.fromEither(s.deserialze[T](bytes)))

  def getKey[T: ClassTag](key: Array[Byte]): IO[StorageException, Option[T]] =
    withDB(_.get(key))
      .flatMap(
        Option(_).fold[IO[SerializationException, Option[T]]](ZIO.succeed(None))(bytes =>
          deserialize[T](bytes).map(Option(_))
        )
      )
      .mapError(e => StorageException(s"Get key ${new String(key)} fromm RocksDB failed", Option(e)))

  def getKeys[T: ClassTag](keys: List[Array[Byte]]): IO[StorageException, List[T]] =
    withDB(_.multiGetAsList(keys.asJava))
      .flatMap(m => ZIO.collectAll(m.asScala.toList.map(v => deserialize[T](v))))
      .mapError(e => StorageException(s"Get keys fromm RocksDB failed", Option(e)))

  def putKey[T: ClassTag](key: Array[Byte], value: T): IO[StorageException, Unit] =
    serdeRef.get.flatMap(serde => withDB(_.put(key, serde.serialize(value))))

  def deleteKey(key: Array[Byte]): IO[StorageException, Unit] =
    withDB(_.delete(key))

  def deleteKeys(keys: List[Array[Byte]]): IO[StorageException, Unit] =
    ZIO.collectAll(keys.map(deleteKey)).unit

}
