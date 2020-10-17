package com.ariskk.raft.model

sealed trait Command extends Serializable
trait ReadCommand    extends Command
trait WriteCommand   extends Command
