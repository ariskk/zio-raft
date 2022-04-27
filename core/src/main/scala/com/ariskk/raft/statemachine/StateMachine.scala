package com.ariskk.raft.statemachine

import zio._

import com.ariskk.raft.model.Command.{ ReadCommand, WriteCommand }
import com.ariskk.raft.model.RaftException.StateMachineException

/**
 * Very simplistic modeling
 */
trait StateMachine[T] {
  def write: PartialFunction[WriteCommand, IO[StateMachineException, Unit]]
  def read: PartialFunction[ReadCommand, IO[StateMachineException, Option[T]]]
}
