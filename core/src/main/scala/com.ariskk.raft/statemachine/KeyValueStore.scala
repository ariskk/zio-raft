package com.ariskk.raft.statemachine

import zio.stm._

import com.ariskk.raft.model._

final case class Key(value: String)              extends AnyVal
final case class ReadKey(key: Key)               extends ReadCommand
final case class WriteKey[T](key: Key, value: T) extends WriteCommand

final class KeyValueStore[T](map: TMap[Key, T]) extends StateMachine[T] {
  def write = { case WriteKey(key: Key, value: T) => map.put(key, value) }
  def read  = { case ReadKey(key) => map.get(key) }
}

object KeyValueStore {
  def apply[R] = TMap.empty[Key, R].commit.map(new KeyValueStore(_))
}
