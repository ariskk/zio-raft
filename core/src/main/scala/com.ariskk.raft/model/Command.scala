package com.ariskk.raft.model

sealed trait Command
trait ReadCommand  extends Command
trait WriteCommand extends Command
