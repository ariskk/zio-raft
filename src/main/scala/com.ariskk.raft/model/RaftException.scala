package com.ariskk.raft.model

sealed trait RaftException                        extends Throwable
case object QueueFullError                        extends RaftException
case class InvalidStateException(message: String) extends RaftException
