package com.ariskk.raft

import scala.util.Random

import zio._
import zio.stm._
import zio.duration._

import com.ariskk.raft.model._
import Message._

final class TestCluster(nodeRef: TRef[Map[RaftNode.Id, Raft]], chaos: Boolean) {

  def getNode(id: RaftNode.Id) = for {
    nodes <- getNodeMap
    node <- ZIO.fromOption(nodes.get(id))
  } yield node

  def getNodeMap: UIO[Map[RaftNode.Id, Raft]] = nodeRef.get.commit

  def addNode(node: Raft) = (for {
    nodes <- nodeRef.get
    nodeId = node.nodeId
    updatedNodes = nodes.removed(nodeId) + (nodeId -> node)
    _ <- nodeRef.set(updatedNodes)
  } yield ()).commit

  def removeNode(nodeId: RaftNode.Id) = for {
    nodes <- nodeRef.get
    _ <- nodeRef.set(nodes.removed(nodeId))
  } yield ()

  // Can be more than one
  def getLeaders: UIO[Iterable[Raft]] = for {
    nodes <- nodeRef.get.commit
    nodeData <- ZIO.collectAll(nodes.values.map(_.node))
    leaderIds = nodeData.filter(_.isLeader).map(_.id).toSet
    leaders = nodes.values.filter { n => leaderIds.contains(n.nodeId) }
  } yield leaders

  def getNodeStates: UIO[Iterable[NodeState]] = for {
    nodes <- nodeRef.get.commit
    nodeData <- ZIO.collectAll(nodes.values.map(_.node))
    states = nodeData.map(_.state)
  } yield states

  private def sendMessage(m: Message) = for {
    node <- getNode(m.to)
    _ <- m match {
      case v: VoteRequest   => node.offerVoteRequest(v)
      case v: VoteResponse => node.offerVote(v)
      case h: Heartbeat => node.offerHeartbeat(h)
      case a: HeartbeatAck => node.offerHeartbeatAck(a)
      case _ => ZIO.die(new UnsupportedOperationException("Message type not supported"))
    }
  } yield ()

  /**
   * Simulates a shitty network.
   * Your network might not be better than this.
  */
  private def networkChaos(messages: Iterable[Message]) = {
    val shuffled = scala.util.Random.shuffle(messages)
    val ios = shuffled.map(sendMessage)
    val delayedIOs = ios.map(_.delay(Random.nextInt(5).milliseconds))
    if (Random.nextInt(10) > 8) delayedIOs.dropRight(1) else delayedIOs
  }

  def run = {

    lazy val startNodes = for {
      nodes <- nodeRef.get.commit
      _ <- ZIO.collectAllPar_(
        nodes.values.map(_.run)
      )
    } yield ()

    lazy val program = for {
      nodes <- nodeRef.get.commit
      allMsgs <- ZIO.collectAll(nodes.values.map(_.takeAll)).map(_.flatten)
      msgIOs = if (chaos) networkChaos(allMsgs) else allMsgs.map(sendMessage)
      _ <- ZIO.collectAll(msgIOs)
    } yield ()

    startNodes <&> program.forever

  }
}

object TestCluster {

  def apply(numberOfNodes: Int,  chaos: Boolean = false): UIO[TestCluster] = {
    val nodeIds = (1 to numberOfNodes).map(_ => RaftNode.newUniqueId).toSet
  
    for {
      nodes <- ZIO.collectAll(nodeIds.map(id => Raft(id, nodeIds - id)))
      nodeRef <- TRef.makeCommit(nodes.map(n => (n.nodeId -> n)).toMap)
    } yield new TestCluster(nodeRef, chaos)
  }
}
