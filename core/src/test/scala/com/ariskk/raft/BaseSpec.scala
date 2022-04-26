package com.ariskk.raft

import com.ariskk.raft.model.NodeState
import com.ariskk.raft.model.NodeState.{ Follower, Leader }
import com.ariskk.raft.statemachine.{ Key, WriteKey }
import zio.{ Fiber, ZIO }
import zio.duration.durationInt
import zio.test.environment.{ live, Live }
import zio.test.{ DefaultRunnableSpec, TestAspect }

trait BaseSpec extends DefaultRunnableSpec {

  override def aspects = List(TestAspect.timeout(2.seconds))

  def sameState(s1: Seq[NodeState], s2: Seq[NodeState]): Boolean =
    s1.diff(s2).isEmpty && s2.diff(s1).isEmpty

  def liveCluster[T](
    nodes: Int,
    chaos: Boolean
  ): ZIO[Live, Nothing, (TestCluster[T], Fiber.Runtime[Serializable, (Unit, Nothing)])] = for {
    cluster <- TestCluster.apply[T](numberOfNodes = nodes, chaos = chaos)
    fiber   <- live(cluster.run.fork)
    states <- cluster.getNodeStates.repeatUntil { ns =>
      sameState(ns.toSeq, (1 until nodes).map(_ => Follower) :+ Leader)
    }
  } yield (cluster, fiber)

  lazy val unitCommand = WriteKey[Unit](Key("key"), ())

  def intCommand(i: Int): WriteKey[Int] = WriteKey[Int](Key(s"key$i"), i)

}
