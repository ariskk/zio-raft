package com.ariskk.raft

import zio.test.{ DefaultRunnableSpec, _ }
import zio.duration._
import zio.test.environment._

import com.ariskk.raft.model._
import com.ariskk.raft.statemachine._
import NodeState._

trait BaseSpec extends DefaultRunnableSpec {

  override def aspects = List(TestAspect.timeout(2.seconds))

  def sameState(s1: Seq[NodeState], s2: Seq[NodeState]): Boolean =
    s1.diff(s2).isEmpty && s2.diff(s1).isEmpty

  def liveCluster[T](nodes: Int, chaos: Boolean) = for {
    cluster <- TestCluster.apply[T](numberOfNodes = nodes, chaos = chaos)
    fiber   <- live(cluster.run.fork)
    states <- cluster.getNodeStates.repeatUntil { ns =>
      sameState(ns.toSeq, (1 to nodes - 1).map(_ => Follower) :+ Leader)
    }
  } yield (cluster, fiber)

  lazy val unitCommand = WriteKey[Unit](Key("key"), ())

  def intCommand(i: Int) = WriteKey[Int](Key(s"key$i"), i)

}
