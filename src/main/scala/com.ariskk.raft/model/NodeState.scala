package com.ariskk.raft.model

sealed trait NodeState

object NodeState {

  case object Follower extends NodeState
  case object Candidate extends NodeState
  case object Leader extends NodeState

  lazy val default = Follower

}

