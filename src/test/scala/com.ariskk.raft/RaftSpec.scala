package com.ariskk.raft

import zio.test.{ DefaultRunnableSpec, _ }
import zio.test.Assertion._
import zio.duration._
import zio.test.environment._

import com.ariskk.raft.model._

object RaftSpec extends DefaultRunnableSpec {

  override def aspects = List(TestAspect.timeout(3.seconds))

  def spec = suite("RaftSpec")(
    testM("By default a node should be in Follower state") {

      lazy val program = for {
        raft  <- TestRaft.default[Unit]
        state <- raft.nodeState
      } yield state

      assertM(program)(equalTo(NodeState.Follower))
    },
    testM("It should become leader if it runs alone") {

      lazy val program = for {
        raft  <- TestRaft.default[Unit]
        _     <- raft.runForLeader.fork
        _     <- TestClock.adjust(1.second)
        state <- raft.nodeState
      } yield state

      assertM(program)(equalTo(NodeState.Leader))

    },
    testM("It should become leader if starts as a follower and doesn't receive a hearbeat") {

      lazy val program = for {
        raft  <- TestRaft.default[Unit]
        _     <- raft.runFollowerLoop.fork
        _     <- TestClock.adjust(1.second)
        state <- raft.nodeState
      } yield state

      assertM(program)(equalTo(NodeState.Leader))

    },
    testM("It should be able to add and remove peers") {

      val newPeer = NodeId.newUniqueId

      lazy val program = for {
        raft             <- TestRaft.default[Unit]
        _                <- raft.addPeer(newPeer)
        peersWithNewPeer <- raft.peers.commit
        _                <- raft.removePeer(newPeer)
        peersWithout     <- raft.peers.commit
      } yield (peersWithNewPeer, peersWithout)

      assertM(program)(equalTo((List(newPeer), List.empty[NodeId])))

    }
  )
}
