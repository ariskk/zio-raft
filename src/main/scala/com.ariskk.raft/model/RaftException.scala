package com.ariskk.raft.model

sealed trait RaftException                          extends Throwable
case class InvalidStateException(message: String)   extends RaftException
case class InvalidCommandException(message: String) extends RaftException
case class StorageException(message: String)        extends RaftException
case object LeaderNotFoundException                 extends RaftException
