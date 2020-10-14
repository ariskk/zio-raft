package com.ariskk.raft

import zio.test._
import zio.test.Assertion._
import zio.duration._
import zio._

import com.ariskk.raft.statemachine._

/**
 * Those tests use the live clock and emulate a faulty network.
 * `TestCluster` will a) shuffle messages b) drop messages c) delay messages
 * if `chaos = true`. This is to test protocol resilience under realistic conditions.
 */
object ClusterSpec extends BaseSpec {

  override def aspects = List(TestAspect.timeout(5.seconds))

  def spec = suite("ClusterSpec")(
    testM("A three node cluster should be able to elect a single leader") {

      lazy val program = liveCluster[Unit](3, chaos = false).flatMap { case (_, fiber) => fiber.interrupt }.unit

      assertM(program)(equalTo(()))

    },
    testM("Even during adverse network conditions") {

      lazy val program = liveCluster[Unit](3, chaos = true).flatMap { case (_, fiber) => fiber.interrupt }.unit

      assertM(program)(equalTo(()))

    },
    testM("A client should be able to submit a command") {

      lazy val program = for {
        (cluster, fiber) <- liveCluster[Unit](3, chaos = false)
        _                <- cluster.submitCommand(unitCommand)
        _ <- cluster.getAllLogEntries.repeatUntil { case (_, entries) =>
          entries.map(_.map(_.command)) == Seq(Seq(unitCommand), Seq(unitCommand), Seq(unitCommand))
        }
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo(()))
    },
    testM("Commands must be appeneded in the order they were commited") {

      lazy val program = for {
        (cluster, fiber) <- liveCluster[Int](3, chaos = false)
        _                <- ZIO.collectAll((1 to 5).map(i => cluster.submitCommand(intCommand(i))))
        correctLog = (1 to 5).toSeq.map(intCommand)
        _ <- cluster.getAllLogEntries.repeatUntil { case (_, entries) =>
          entries.map(_.map(_.command)) == Seq(correctLog, correctLog, correctLog)
        }
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo(()))

    },
    testM("Order should be preserved even during averse network conditions") {

      lazy val program = for {
        (cluster, fiber) <- liveCluster[Int](3, chaos = true)
        commands = (1 to 3).map(intCommand)
        _ <- ZIO.collectAll(commands.map(cluster.submitCommand))
        _ <- cluster.getAllLogEntries.repeatUntil { case (_, entries) =>
          entries.map(_.map(_.command)) == Seq(commands, commands, commands)
        }
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo(()))

    },
    testM("All committed entries should be applied to the state machine") {

      lazy val program = for {
        (cluster, fiber) <- liveCluster[Int](3, chaos = false)
        commands = (1 to 5).map(intCommand)
        _ <- ZIO.collectAll(commands.map(cluster.submitCommand))
        _ <- cluster.getAllLogEntries.repeatUntil { case (_, entries) =>
          entries.map(_.map(_.command)) == (1 to 3).map(_ => commands).toSeq
        }
        _ <- ZIO.collectAll(
          (1 to 5).map(i =>
            cluster.queryStateMachines(ReadKey(Key(s"key$i"))).repeatUntil { results =>
              results == Seq(Some(i), Some(i), Some(i))
            }
          )
        )
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo(()))

    },
    testM("All committed entries should be applied even during adverse network conditions") {

      lazy val program = for {
        (cluster, fiber) <- liveCluster[Int](3, chaos = true)
        _                <- ZIO.collectAll((1 to 5).map(intCommand).map(cluster.submitCommand))
        _ <- ZIO.collectAll(
          (1 to 5).map(i =>
            cluster.queryStateMachines(ReadKey(Key(s"key$i"))).repeatUntil { results =>
              results == Seq(Some(i), Some(i), Some(i))
            }
          )
        )
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo(()))

    }
  )

}
