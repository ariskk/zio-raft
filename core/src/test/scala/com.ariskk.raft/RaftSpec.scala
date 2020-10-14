package com.ariskk.raft

import zio.test._
import zio.test.Assertion._
import zio.duration._
import zio.test.environment._

import com.ariskk.raft.model._
import Message._

object RaftSpec extends BaseSpec {

  override def aspects = List(TestAspect.timeout(3.seconds))

  def spec = suite("RaftSpec")(
    testM("By default a node should be in Follower state") {

      lazy val program = for {
        raft  <- TestRaft.default[Unit]
        state <- raft.nodeState
      } yield state

      assertM(program)(equalTo(NodeState.Follower))
    },
    testM("It should become leader if it runs alone") {

      lazy val program = for {
        raft  <- TestRaft.default[Unit]
        _     <- raft.runForLeader.fork
        _     <- TestClock.adjust(1.second)
        state <- raft.nodeState
      } yield state

      assertM(program)(equalTo(NodeState.Leader))

    },
    testM("It should become leader if starts as a follower and doesn't receive a hearbeat") {

      lazy val program = for {
        raft  <- TestRaft.default[Unit]
        _     <- raft.runFollowerLoop.fork
        _     <- TestClock.adjust(1.second)
        state <- raft.nodeState
      } yield state

      assertM(program)(equalTo(NodeState.Leader))

    },
    testM("It should be able to add and remove peers") {

      val newPeer = NodeId.newUniqueId

      lazy val program = for {
        raft             <- TestRaft.default[Unit]
        _                <- raft.addPeer(newPeer)
        peersWithNewPeer <- raft.peers
        _                <- raft.removePeer(newPeer)
        peersWithout     <- raft.peers
      } yield (peersWithNewPeer, peersWithout)

      assertM(program)(equalTo((List(newPeer), List.empty[NodeId])))

    },
    testM("It should reject vote requests from out of date nodes") {

      val peer = NodeId.newUniqueId

      lazy val program = for {
        raft  <- TestRaft.default[Int]
        fiber <- raft.runFollowerLoop.fork
        _     <- raft.appendEntry(Index(0), LogEntry(intCommand(0), Term(1)))
        _     <- raft.appendEntry(Index(1), LogEntry(intCommand(1), Term(3)))
        // Last entry has an older term
        firstRequest = VoteRequest(peer, raft.nodeId, Term(4), Index(1), Term(2))
        // Candidate log is smaller than follower log
        secondRequest = VoteRequest(peer, raft.nodeId, Term(4), Index(0), Term(1))
        _ <- raft.offerVoteRequest(firstRequest)
        _ <- raft.poll.repeatUntil { response =>
          response match {
            case Some(VoteResponse(_, _, _, granted)) => granted == false
            case _                                    => false
          }
        }
        _ <- raft.offerVoteRequest(secondRequest)
        _ <- raft.poll.repeatUntil { response =>
          response match {
            case Some(VoteResponse(_, _, _, granted)) => granted == false
            case _                                    => false
          }
        }
        _ <- fiber.interrupt
      } yield ()

      assertM(program)(equalTo())

    }
  )
}
