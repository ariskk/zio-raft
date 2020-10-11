package com.ariskk.raft.statemachine

import zio.stm._

import com.ariskk.raft.model._

/**
 * Very simplistic modeling
 */
trait StateMachine[T] {
  def write: PartialFunction[WriteCommand, STM[StateMachineException, Unit]]
  def read: PartialFunction[ReadCommand, STM[StateMachineException, Option[T]]]
}
