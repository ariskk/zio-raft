package com.ariskk.raft.rocksdb

import java.util.UUID

import zio.ZIO
import zio.duration._
import zio.test.Assertion._
import zio.test.environment._
import zio.test.{ DefaultRunnableSpec, _ }

import com.ariskk.raft._
import com.ariskk.raft.model.NodeState._
import com.ariskk.raft.model._
import com.ariskk.raft.statemachine._

object RocksDBSpec extends DefaultRunnableSpec {

  override def aspects = List(TestAspect.timeout(10.seconds))

  private def createStorage =
    RocksDBStorage.apply(s"/tmp/rocks-${UUID.randomUUID().toString.take(10)}", "DB")

  def spec = suite("RocksDBSpec")(
    testM("Should store terms") {

      lazy val program = for {
        storage <- createStorage
        term    <- storage.getTerm.repeatUntil(_ == Term.Zero)
        newTerm = term.increment
        _ <- storage.storeTerm(newTerm)
        _ <- storage.getTerm.repeatUntil(_ == newTerm)
      } yield ()

      assertM(program)(equalTo())

    },
    testM("Should store votes") {

      lazy val program = for {
        storage <- createStorage
        vote = Vote(NodeId.newUniqueId, Term(1))
        _ <- storage.storeVote(vote)
        _ <- storage.getVote.repeatUntil(_ == Option(vote))
        newVote = vote.copy(term = Term(2))
        _ <- storage.storeVote(newVote)
        _ <- storage.getVote.repeatUntil(_ == Option(newVote))
      } yield ()

      assertM(program)(equalTo())

    },
    testM("Should able to append, get a range and purge") {

      val term = Term(1)

      lazy val program = for {
        storage <- createStorage
        _ <- (storage.lastIndex <*> storage.logSize).repeatUntil(
          _ == (Index(-1), 0)
        )
        entries = (1 to 5).map(i => LogEntry(WriteKey(Key(s"key$i"), i), term))
        _ <- ZIO.collectAll(entries.zipWithIndex.map { case (e, i) =>
          storage.appendEntry(Index(i), e)
        })
        _ <- (storage.lastIndex <*> storage.logSize).repeatUntil(
          _ == (Index(4), 5)
        )
        last <- storage.lastIndex
        _ <- storage
          .getRange(last.decrement, last)
          .repeatUntil(
            _ == entries.takeRight(2)
          )
        _ <- storage.purgeFrom(last.decrement)
        _ <- storage.lastIndex.repeatUntil(_ == last.decrement.decrement)
        _ <- storage
          .getRange(Index(0), Index(2))
          .repeatUntil(
            _ == entries.take(3)
          )
      } yield ()

      assertM(program)(equalTo())

    },
    testM("It should be able to work in a Raft cluster") {

      val nodeIds = (1 to 3).map(_ => NodeId.newUniqueId).toSet

      def rocksNode[T](id: NodeId, peers: Set[NodeId]) = for {
        s    <- createStorage
        sm   <- KeyValueStore.apply[T]
        raft <- Raft[T](id, peers, s, sm)
      } yield raft

      lazy val program = for {
        nodes   <- ZIO.collectAll(nodeIds.map(id => rocksNode[Int](id, nodeIds - id)))
        cluster <- TestCluster.forNodes[Int](nodes.toSeq, chaos = false)
        fiber   <- live(cluster.run.fork)
        states <- cluster.getNodeStates.repeatUntil { ns =>
          ns.count(_ == Leader) == 1 &&
          ns.count(_ == Follower) == 2
        }
        commands = (1 to 5).map(i => WriteKey(Key(s"key$i"), i))
        _ <- ZIO.collectAll(commands.map(cluster.submitCommand))
        _ <- ZIO.collectAll(
          (1 to 5).map(i =>
            cluster.queryStateMachines(ReadKey(Key(s"key$i"))).repeatUntil { results =>
              results == Seq(Some(i), Some(i), Some(i))
            }
          )
        )
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo())
    }
  )
}
