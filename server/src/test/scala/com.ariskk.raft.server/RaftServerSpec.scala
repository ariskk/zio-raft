package com.ariskk.raft.server

import zio.test.{ DefaultRunnableSpec, _ }
import zio.test.Assertion._
import zio.test.environment._
import zio.duration._
import zio.nio.core._
import zio.ZIO

import com.ariskk.raft.model._
import com.ariskk.raft.statemachine._
import com.ariskk.raft.rocksdb._

object RaftServerSpec extends DefaultRunnableSpec {

  override def aspects = List(TestAspect.timeout(5.seconds))

  private lazy val serde = Serde.kryo

  private def createStorage(nodeId: NodeId) =
    RocksDBStorage.apply(s"/tmp/rocks-${nodeId.value}", "DB")

  private def createRaftServer[T](config: RaftServer.Config, peers: Set[RaftServer.Config]) = for {
    storage      <- createStorage(config.nodeId)
    stateMachine <- KeyValueStore.apply[T]
    server       <- RaftServer(config, peers.toSeq, storage, stateMachine)
  } yield server

  private def createClient(nodes: Seq[RaftServer.Config]) = RaftClient(nodes)

  private def generateConfig(serverPort: Int, clientPort: Int) = InetAddress.localHost.map(a =>
    RaftServer.Config(
      NodeId.newUniqueId,
      a,
      raftPort = serverPort,
      raftClientPort = clientPort
    )
  )

  private def generateConfigs(numberOfNodes: Int) = ZIO.collectAll(
    (1 to numberOfNodes).map(i => generateConfig(9700 + i, 9800 + i))
  )

  def spec = suite("RaftServerSpec")(
    testM("It should elect a leader, accept commands and respond to queries") {
      lazy val program = for {
        configs <- generateConfigs(3)
        servers <- ZIO.collectAll(configs.map(c => createRaftServer[Int](c, configs.toSet - c)))
        client  <- createClient(configs)
        fibers  <- live(ZIO.collectAll(servers.map(_.run.fork)))
        _ <- ZIO.collectAll(servers.map(_.getState)).repeatUntil { states =>
          states.filter(_ == NodeState.Leader).size == 1 &&
          states.filter(_ == NodeState.Follower).size == 2
        }
        _ <- ZIO.collectAll((1 to 5).map(i => live(client.submitCommand(WriteKey(Key(s"key-$i"), i)))))
        _ <- ZIO.collectAll(
          (1 to 5).map(i =>
            client.submitQuery(ReadKey(Key(s"key-$i"))).repeatUntil { result =>
              result == Some(i)
            }
          )
        )
        _ <- ZIO.collectAll(fibers.map(_.interrupt))
      } yield ()

      assertM(program)(equalTo())
    }
  )

}
