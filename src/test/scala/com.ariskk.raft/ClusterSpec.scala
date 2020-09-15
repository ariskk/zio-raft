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

/**
 * Those tests use the live clock and emulate a faulty network.
 * `TestCluster` will a) shuffle messages b) drop messages c) delay messages
 * if `chaos = true` is set. This is to test protocol resilience under realistic conditions.
*/
object ClusterSpec extends DefaultRunnableSpec {

  override def aspects = List(TestAspect.timeout(10.seconds))

  def spec = suite("ClusterSpec")(
    testM("A three node cluster should be able to elect a single leader") {

      lazy val program = for {
        cluster <- TestCluster(numberOfNodes = 3)
        fiber <- live(cluster.run.fork <* ZIO.sleep(1.second))
        states <- cluster.getNodeStates
        _ <- fiber.interrupt
      } yield states

      assertM(program)(hasSameElements(List(NodeState.Leader, NodeState.Follower, NodeState.Follower)))

    },
    testM("Even on adverse network conditions") {

      lazy val program = for {
        cluster <- TestCluster(numberOfNodes = 3, chaos = true)
        fiber <- live(cluster.run.fork <* ZIO.sleep(1.second))
        states <- cluster.getNodeStates
        _ <- fiber.interrupt
      } yield states

      assertM(program)(hasSameElements(List(NodeState.Leader, NodeState.Follower, NodeState.Follower)))

    }
  )

}