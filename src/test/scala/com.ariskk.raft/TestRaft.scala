package com.ariskk.raft

import zio._

import com.ariskk.raft.model._

/**
 * Generates Raft instances using MemoryStorage
 */
object TestRaft {

  def default[T]: UIO[Raft[T]] = {
    val id = RaftNode.newUniqueId
    for {
      s    <- MemoryStorage.default[T]
      raft <- Raft.default[T](s)
    } yield raft
  }

  def apply[T](nodeId: RaftNode.Id, peers: Set[RaftNode.Id]): UIO[Raft[T]] = for {
    s    <- MemoryStorage.default[T]
    raft <- Raft[T](nodeId, peers, s)
  } yield raft

}
