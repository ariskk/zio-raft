package com.ariskk.raft.model

import scala.util._
import scala.reflect.ClassTag

import shapeless.syntax.typeable._
import com.twitter.chill.ScalaKryoInstantiator

trait Serde {
  def serialize[T: ClassTag](t: T): Array[Byte]
  def deserialize[T: ClassTag](bytes: Array[Byte]): Either[SerializationException, T]
}

object Serde {
  def kryo = new Serde {
    private lazy val kryo = ScalaKryoInstantiator.defaultPool

    override def serialize[T: ClassTag](t: T): Array[Byte] = kryo.toBytesWithClass(t)
    override def deserialize[T: ClassTag](bytes: Array[Byte]): Either[SerializationException, T] =
      Try(kryo.fromBytes(bytes)) match {
        case Success(value) =>
          value
            .cast[T]
            .fold[Either[SerializationException, T]](
              Left(SerializationException(s"Failed to cast bytes to ${implicitly[ClassTag[T]]}", None))
            )(Right(_))
        case Failure(e) => Left(SerializationException("Deserialisation failled", Option(e)))
      }
  }
}
