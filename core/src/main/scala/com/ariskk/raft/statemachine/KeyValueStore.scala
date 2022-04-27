package com.ariskk.raft.statemachine

import zio._

import com.ariskk.raft.model.Command.{ ReadCommand, WriteCommand }

final case class Key(value: String)              extends AnyVal
final case class ReadKey(key: Key)               extends ReadCommand
final case class WriteKey[T](key: Key, value: T) extends WriteCommand

final class KeyValueStore[T](map: Ref[Map[Key, T]]) extends StateMachine[T] {
  def write = { case WriteKey(key: Key, value: T) => map.update(_ + (key -> value)) }
  def read  = { case ReadKey(key) => map.get.map(_.get(key)) }
}

object KeyValueStore {
  def apply[R]: ZIO[Any, Nothing, KeyValueStore[R]] = Ref.make(Map.empty[Key, R]).map(new KeyValueStore(_))
}
