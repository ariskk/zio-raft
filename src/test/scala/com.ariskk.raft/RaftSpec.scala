package com.ariskk.raft

import zio.test.{DefaultRunnableSpec, _}
import zio.test.Assertion._
import zio.duration._
import zio.ZIO
import zio.test.environment._
import zio.clock._

import com.ariskk.raft.model._
import Message._
import com.ariskk.raft.Raft

object RaftSpec extends DefaultRunnableSpec {

  override def aspects = List(TestAspect.timeout(3.seconds))

  def spec = suite("RaftSpec")(
    testM("By default a node should be in Follower state") {
  
      lazy val program = for {
        raft <- Raft.default
        state <- raft.nodeState
      } yield state

      assertM(program)(equalTo(NodeState.Follower))
    },
    testM("It should be able to become a candidate") {

      lazy val program = for {
        raft <- Raft(RaftNode.newUniqueId, Set(RaftNode.newUniqueId))
        _ <- raft.becomeCandidate.commit
        state <- raft.nodeState
      } yield state

      assertM(program)(equalTo(NodeState.Candidate))

    },
    testM("It should become leader if it runs alone") {

      lazy val program = for {
        raft <- Raft.default
        _ <- raft.runForLeader.fork
        _ <- TestClock.adjust(1.second)
        state <- raft.nodeState
      } yield state

      assertM(program)(equalTo(NodeState.Leader))

    },
    testM("It should become leader if starts as a follower and doesn't receive a hearbeat") {

      lazy val program = for {
        raft <- Raft.default
        _ <- raft.runFollowerLoop.fork
        _ <- TestClock.adjust(1.second)
        state <- raft.nodeState
      } yield state

      assertM(program)(equalTo(NodeState.Leader))

    }
  )
}
