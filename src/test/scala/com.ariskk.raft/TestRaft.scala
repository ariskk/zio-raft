package com.ariskk.raft

import zio._

import com.ariskk.raft.model._
import com.ariskk.raft.storage.MemoryStorage

/**
 * Generates Raft instances using MemoryStorage
 */
object TestRaft {

  def default[T]: UIO[Raft[T]] = {
    val id = NodeId.newUniqueId
    for {
      s    <- MemoryStorage.default[T]
      raft <- Raft.default[T](s)
    } yield raft
  }

  def apply[T](nodeId: NodeId, peers: Set[NodeId]): UIO[Raft[T]] = for {
    s    <- MemoryStorage.default[T]
    raft <- Raft[T](nodeId, peers, s)
  } yield raft

}
