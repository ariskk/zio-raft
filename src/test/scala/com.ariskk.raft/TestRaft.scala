package com.ariskk.raft

import zio._

import com.ariskk.raft.model._
import com.ariskk.raft.storage.MemoryStorage
import com.ariskk.raft.statemachine.KeyValueStore

/**
 * Generates Raft instances using MemoryStorage and KeyValueStore
 */
object TestRaft {

  def default[T]: UIO[Raft[T]] = {
    val id = NodeId.newUniqueId
    for {
      s    <- MemoryStorage.default[T]
      sm <- KeyValueStore.apply[T]
      raft <- Raft.default[T](s, sm)
    } yield raft
  }

  def apply[T](nodeId: NodeId, peers: Set[NodeId]): UIO[Raft[T]] = for {
    s    <- MemoryStorage.default[T]
    sm <- KeyValueStore.apply[T]
    raft <- Raft[T](nodeId, peers, s, sm)
  } yield raft

}
