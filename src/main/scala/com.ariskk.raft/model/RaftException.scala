package com.ariskk.raft.model

sealed trait RaftException                        extends Throwable
case class InvalidStateException(message: String) extends RaftException
