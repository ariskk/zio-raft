package com.ariskk.raft.model

sealed trait RaftException extends Exception
object RaftException {
  final case class InvalidStateException(message: String) extends Exception(message) with RaftException

  final case class InvalidCommandException(message: String) extends Exception(message) with RaftException

  final case class StorageException(message: String, causedBy: Option[Throwable])
      extends Exception(message, causedBy.orNull)
      with RaftException

  final case object LeaderNotFoundException extends Exception("Leader not found") with RaftException

  final case class StateMachineException(message: String) extends Exception(message) with RaftException

  final case class SerializationException(message: String, causedBy: Option[Throwable])
      extends Exception(message, causedBy.orNull)
      with RaftException
}
