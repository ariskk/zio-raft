package com.ariskk.raft.model

case class Key(value: String) extends AnyVal

sealed trait Command[T]
case class ReadCommand[T](key: Key)            extends Command[T]
case class WriteCommand[T](key: Key, value: T) extends Command[T]
