package com.ariskk.raft.statemachine

import zio.stm._

import com.ariskk.raft.model._

/**
 * Very simplistic modeling
 */
trait StateMachine[T] {
  def write(command: WriteCommand[T]): STM[StateMachineException, Unit]
  def read(command: ReadCommand[T]): STM[StateMachineException, Option[T]]
}
