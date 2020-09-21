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
import NodeState.{Leader, Follower}

/**
 * Those tests use the live clock and emulate a faulty network.
 * `TestCluster` will a) shuffle messages b) drop messages c) delay messages
 * if `chaos = true` is set. This is to test protocol resilience under realistic conditions.
*/
object ClusterSpec extends DefaultRunnableSpec {

  override def aspects = List(TestAspect.timeout(10.seconds))

  private def sameState(s1: Seq[NodeState], s2: Seq[NodeState]): Boolean = 
    s1.diff(s2).isEmpty && s2.diff(s1).isEmpty

  def spec = suite("ClusterSpec")(
    testM("A three node cluster should be able to elect a single leader") {

      lazy val program = for {
        cluster <- TestCluster(numberOfNodes = 3)
        fiber <- live(cluster.run.fork)
        states <- cluster.getNodeStates.repeatUntil(ns =>
          sameState(ns.toSeq, Seq(Leader, Follower, Follower))
        )
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo(()))

    },
    testM("Even on adverse network conditions") {

      lazy val program = for {
        cluster <- TestCluster(numberOfNodes = 3, chaos = true)
        fiber <- live(cluster.run.fork)
        _ <- cluster.getNodeStates.repeatUntil(ns =>
          sameState(ns.toSeq, Seq(Leader, Follower, Follower))
        )
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo(()))

    },
    testM("New peers should be able to join a running cluster") {

      lazy val program = for {
        cluster <- TestCluster(numberOfNodes = 3, chaos = true)
        fiber <- live(cluster.run.fork)
        states <- cluster.getNodeStates.repeatUntil(ns =>
          sameState(ns.toSeq, Seq(Follower, Follower, Leader))
        )
        _ <- cluster.addNewPeer
        states <- cluster.getNodeStates.repeatUntil(ns =>
          sameState(ns.toSeq, Seq(Follower, Follower, Follower, Leader))
        )
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo(()))

    }
  )

}