package com.ariskk.raft.model

sealed trait CommandResponse
case object Committed                 extends CommandResponse
case class Redirect(leaderId: NodeId) extends CommandResponse
