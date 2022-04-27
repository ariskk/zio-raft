package com.ariskk.raft.server

import java.io.IOException

import scala.reflect.ClassTag
import scala.util.Random

import zio._
import zio.clock.Clock
import zio.duration._
import zio.nio.channels._
import zio.nio.{ InetAddress, InetSocketAddress }

import com.ariskk.raft.model.Command.{ ReadCommand, WriteCommand }
import com.ariskk.raft.model.CommandResponse._
import com.ariskk.raft.model._
final class RaftClient(
  nodes: Seq[RaftServer.Config],
  serdeRef: Ref[Serde],
  leaderRef: Ref[RaftServer.Config]
) {

  private val backOff = Seq(
    10.milliseconds,
    50.milliseconds,
    200.milliseconds,
    1000.milliseconds
  )

  private def shuffleLeaderRef = leaderRef.set(nodes(Random.nextInt(nodes.size)))

  def channel(address: InetAddress, port: Int): ZManaged[Any with Clock, IOException, AsynchronousSocketChannel] =
    AsynchronousSocketChannel.open.mapM { client =>
      for {
        address <- InetSocketAddress.inetAddress(address, port)
        _ <- client
          .connect(address)
          .retry(Schedule.fromDurations(5.milliseconds, backOff: _*))
      } yield client
    }

  def submitCommand(command: WriteCommand): ZIO[Clock, Exception, Unit] = for {
    serde  <- serdeRef.get
    leader <- leaderRef.get
    client = channel(leader.address, leader.raftClientPort)
    chunks <- client.use(c =>
      c.writeChunk(Chunk.fromArray(serde.serialize(command))) *>
        c.readChunk(1000)
    )
    response <- ZIO.fromEither(serde.deserialize[CommandResponse](chunks.toArray))
    _ <- response match {
      case Committed => ZIO.unit
      case LeaderNotFoundResponse =>
        shuffleLeaderRef *> submitCommand(command)
      case Redirect(leaderId) =>
        for {
          newLeader <- ZIO
            .fromOption(nodes.find(_.nodeId == leaderId))
            .mapError(_ => new Exception("Failed to find leader in node config"))
          _ <- leaderRef.set(newLeader)
          _ <- submitCommand(command)
        } yield ()
    }
  } yield ()

  def submitQuery[T: ClassTag](query: ReadCommand): ZIO[Clock, Throwable, Option[T]] = for {
    serde  <- serdeRef.get
    leader <- leaderRef.get
    bytes <- channel(leader.address, leader.raftClientPort).use(c =>
      c.writeChunk(Chunk.fromArray(serde.serialize(query))) *>
        c.readChunk(1000)
    )
    response <- ZIO.effect(serde.deserialize[Option[T]](bytes.toArray).toOption.flatten)
  } yield response

}

object RaftClient {
  def apply(nodes: Seq[RaftServer.Config]): IO[Option[Nothing], RaftClient] = for {
    serdeRef  <- Ref.make(Serde.kryo)
    leader    <- ZIO.fromOption(nodes.headOption)
    leaderRef <- Ref.make(leader)
  } yield new RaftClient(nodes, serdeRef, leaderRef)
}
