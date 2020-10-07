package com.ariskk.raft.statemachine

import zio.stm._

import com.ariskk.raft.model._

final class KeyValueStore[T](map: TMap[Key, T]) extends StateMachine[T] {
  override def write(command: WriteCommand[T]) = map.put(command.key, command.value)
  override def read(command: ReadCommand[T])   = map.get(command.key)
}

object KeyValueStore {
  def apply[R] = TMap.empty[Key, R].commit.map(new KeyValueStore(_))
}
