package com.ariskk.raft

import com.ariskk.raft.model.NodeId
import com.ariskk.raft.statemachine.KeyValueStore
import com.ariskk.raft.storage.MemoryStorage
import zio.UIO

/**
 * Generates Raft instances using MemoryStorage and KeyValueStore
 */
object TestRaft {

  def default[T]: UIO[Raft[T]] = {
    val id = NodeId.newUniqueId
    for {
      s    <- MemoryStorage.default
      sm   <- KeyValueStore.apply[T]
      raft <- Raft.default[T](s, sm)
    } yield raft
  }

  def apply[T](nodeId: NodeId, peers: Set[NodeId]): UIO[Raft[T]] = for {
    s    <- MemoryStorage.default
    sm   <- KeyValueStore.apply[T]
    raft <- Raft[T](nodeId, peers, s, sm)
  } yield raft

}
