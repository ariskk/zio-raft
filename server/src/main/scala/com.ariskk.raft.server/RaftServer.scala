package com.ariskk.raft.server

import zio._
import zio.clock.Clock
import zio.nio.core._
import zio.nio.channels._

import com.ariskk.raft.model._
import com.ariskk.raft.storage._
import com.ariskk.raft.statemachine.StateMachine
import com.ariskk.raft.Raft

final class RaftServer[T](
  config: RaftServer.Config,
  peerConfig: Seq[RaftServer.Config],
  raftRef: Ref[Raft[T]],
  serdeRef: Ref[Serde]
) {

  private[server] def allEntries = raftRef.get.flatMap(_.getAllEntries)

  private[server] def getState = raftRef.get.flatMap(_.nodeState)

  private lazy val clientChannel = AsynchronousServerSocketChannel().mapM { socket =>
    for {
      _ <- SocketAddress.inetSocketAddress(config.raftClientPort) >>= socket.bind
      _ <- socket.accept.preallocate.flatMap(_.use(channel => processCommandChannel(channel)).fork).forever.fork
    } yield ()
  }.useForever

  private def processCommandChannel(channel: AsynchronousSocketChannel) = {
    lazy val program = for {
      chunk <- channel.readChunk(1000) // TODO fold stream
      bytes = chunk.toArray
      responseBytes <- processCommand(bytes)
      _             <- channel.writeChunk(Chunk.fromArray(responseBytes))
    } yield ()

    program.whenM(channel.isOpen).forever
  }

  private def processCommand(bytes: Array[Byte]): ZIO[Clock, RaftException, Array[Byte]] = for {
    serde   <- serdeRef.get
    command <- ZIO.fromEither(serde.deserialize[Command](bytes))
    raft    <- raftRef.get
    response <- command match {
      case w: WriteCommand => raft.submitCommand(w)
      case r: ReadCommand  => raft.submitQuery(r)
    }
  } yield serde.serialize(response)

  private lazy val interServerChannel = AsynchronousServerSocketChannel().mapM { socket =>
    for {
      _ <- SocketAddress.inetSocketAddress(config.raftPort) >>= socket.bind
      _ <- socket.accept.preallocate.flatMap(_.use(channel => processMessageChannel(channel)).fork).forever.fork
    } yield ()
  }.useForever

  private def processMessageChannel(channel: AsynchronousSocketChannel) = {
    lazy val program = for {
      chunk <- channel.readChunk(1000) // TODO fold stream
      bytes = chunk.toArray
      responseBytes <- processMessage(bytes)
    } yield ()

    program.whenM(channel.isOpen).forever
  }

  private def processMessage(bytes: Array[Byte]): ZIO[Clock, RaftException, Unit] = for {
    serde   <- serdeRef.get
    message <- ZIO.fromEither(serde.deserialize[Message](bytes))
    raft    <- raftRef.get
    _       <- raft.offerMessage(message)
  } yield ()

  private lazy val peerChannels = peerConfig.map { peer =>
    (
      peer.nodeId,
      AsynchronousSocketChannel().mapM { client =>
        for {
          address <- SocketAddress.inetSocketAddress(peer.address, peer.raftPort)
          _       <- client.connect(address)
        } yield client
      }
    )
  }.toMap

  private def sendMessage(m: Message) = peerChannels
    .get(m.to)
    .fold[ZIO[Any, Exception, Unit]](ZIO.unit) { channel =>
      for {
        serde <- serdeRef.get
        _     <- channel.use(_.writeChunk(Chunk.fromArray(serde.serialize(m))))
      } yield ()
    }

  private lazy val sendMessages = for {
    raft <- raftRef.get
    _ <- raft.takeAll.flatMap { ms =>
      ZIO.collectAllPar(ms.map(sendMessage))
    }.forever
  } yield ()

  lazy val run = for {
    raft                    <- raftRef.get
    raftFiber               <- raft.run.fork
    clientChannelFiber      <- clientChannel.fork
    interServerChannelFiber <- interServerChannel.fork
    _                       <- sendMessages
  } yield ()

}

object RaftServer {
  case class Config(
    nodeId: NodeId,
    address: InetAddress,
    raftPort: Int,
    raftClientPort: Int
  )

  def apply[T](
    config: Config,
    peerConfig: Seq[Config],
    storage: Storage,
    stateMachine: StateMachine[T]
  ): UIO[RaftServer[T]] = for {
    raft     <- Raft[T](config.nodeId, peerConfig.map(_.nodeId).toSet, storage, stateMachine)
    raftRef  <- Ref.make(raft)
    serdeRef <- Ref.make(Serde.kryo)
  } yield new RaftServer[T](config, peerConfig, raftRef, serdeRef)

}
