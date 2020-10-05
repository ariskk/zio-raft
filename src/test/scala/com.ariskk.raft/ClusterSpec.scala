package com.ariskk.raft

import zio.test.{ DefaultRunnableSpec, _ }
import zio.test.Assertion._
import zio.duration._
import zio.test.environment._
import zio._

import com.ariskk.raft.model._
import NodeState.{ Follower, Leader }

/**
 * Those tests use the live clock and emulate a faulty network.
 * `TestCluster` will a) shuffle messages b) drop messages c) delay messages
 * if `chaos = true` is set. This is to test protocol resilience under realistic conditions.
 */
object ClusterSpec extends DefaultRunnableSpec {

  override def aspects = List(TestAspect.timeout(5.seconds))

  private def sameState(s1: Seq[NodeState], s2: Seq[NodeState]): Boolean =
    s1.diff(s2).isEmpty && s2.diff(s1).isEmpty

  private def liveCluster[T](nodes: Int, chaos: Boolean) = for {
    cluster <- TestCluster.apply[T](numberOfNodes = nodes, chaos = chaos)
    fiber   <- live(cluster.run.fork)
    states <- cluster.getNodeStates.repeatUntil { ns =>
      sameState(ns.toSeq, (1 to nodes - 1).map(_ => Follower) :+ Leader)
    }
  } yield (cluster, fiber)

  def spec = suite("ClusterSpec")(
    testM("A three node cluster should be able to elect a single leader") {

      lazy val program = liveCluster[Unit](3, chaos = false)
        .flatMap { case (_, fiber) => fiber.interrupt }
        .unit

      assertM(program)(equalTo(()))

    },
    testM("Even on adverse network conditions") {

      lazy val program = liveCluster[Unit](3, chaos = true)
        .flatMap { case(_, fiber) => fiber.interrupt }
        .unit

      assertM(program)(equalTo(()))

    },
    testM("A client should be able to submit a command") {

      lazy val program = for {
        (cluster, fiber) <- liveCluster[Unit](3, chaos = false)
        _ <- cluster.submitCommand(())
        _ <- cluster.getAllLogEntries.repeatUntil { case (_, entries) =>
          entries.map(_.map(_.command)) == Seq(Seq(()), Seq(()), Seq(()))
        }
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo(()))
    },
    testM("Commands must be appeneded in the order they were commited") {

      lazy val program = for {
        (cluster, fiber) <- liveCluster[Int](3, chaos = false)
        _ <- ZIO.collectAll((1 to 5).map(i => cluster.submitCommand(i)))
        correctLog = (1 to 5).toSeq
        _ <- cluster.getAllLogEntries.repeatUntil { case (_, entries) =>
          entries.map(_.map(_.command)) == Seq(correctLog, correctLog, correctLog)
        }
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo(()))

    },
    testM("Order should be preserved even when network is faulty") {

      lazy val program = for {
        (cluster, fiber) <- liveCluster[Int](3, chaos = true)
        _ <- ZIO.collectAll((1 to 3).map(i => cluster.submitCommand(i)))
        correctLog = (1 to 3).toSeq
        _ <- cluster.getAllLogEntries.repeatUntil { case (_, entries) =>
          entries.map(_.map(_.command)) == Seq(correctLog, correctLog, correctLog)
        }
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo(()))

    }
  )

}
