package com.ariskk.raft.storage

import zio.stm._

import com.ariskk.raft.model._

/**
 * Interface of persistent server state.
 * Current term, latest node vote and all log entries must be committed here
 * to stable storage before responding to RPC.
 * For more info, look at Figure 2 of https://raft.github.io/raft.pdf
 */
trait Storage[T] {

  def appendEntry(entry: LogEntry[T]): STM[StorageException, Unit]

  def logSize: STM[StorageException, Long]

  def storeVote(node: RaftNode.Id): STM[StorageException, Unit]

  def getVote: STM[StorageException, Option[RaftNode.Id]]

  def storeTerm(term: Term): STM[StorageException, Unit]

  def getTerm: STM[StorageException, Term]

}
