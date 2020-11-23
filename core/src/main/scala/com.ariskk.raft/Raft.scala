package com.ariskk.raft

import zio._
import zio.duration._
import zio.clock._

import com.ariskk.raft.model._
import Message._
import Raft.{ MessageQueues }
import com.ariskk.raft.volatile.VolatileState
import com.ariskk.raft.storage.Storage
import com.ariskk.raft.statemachine.StateMachine

/**
 * Runs the consensus module of a single Raft Node.
 * It communicates with the outside world through message passing, using `Queue` instances.
 * Communication with the outside world is fully asynchronous.
 */
final class Raft[T] private (
  val nodeId: NodeId,
  state: VolatileState,
  storage: Storage,
  queues: MessageQueues,
  stateMachine: StateMachine[T]
) {

  def poll: UIO[Option[Message]] = queues.outboundQueue.poll

  def takeAll: UIO[Seq[Message]] = queues.outboundQueue.takeAll

  def offerMessage(message: Message): UIO[Boolean] = message match {
    case voteRequest: VoteRequest   => queues.inboundVoteRequestQueue.offer(voteRequest)
    case voteResponse: VoteResponse => queues.inboundVoteResponseQueue.offer(voteResponse)
    case entries: AppendEntries     => queues.appendEntriesQueue.offer(entries)
    case entriesResponse: AppendEntriesResponse =>
      queues.appendResponseQueue.offer(entriesResponse)
  }

  def addPeer(peer: NodeId) = state.addPeer(peer)

  def removePeer(peer: NodeId) = state.removePeer(peer)

  private[raft] def getAllEntries = storage.log.getEntries(Index(0))

  /** For testing purposes */
  private[raft] def appendEntry(index: Index, entry: LogEntry) = storage.appendEntry(index, entry)

  private[raft] def sendMessage(m: Message) = queues.outboundQueue.offer(m)

  private[raft] def sendMessages(ms: Iterable[Message]) = queues.outboundQueue.offerAll(ms)

  private[raft] def nodeState: UIO[NodeState] = state.nodeState

  private[raft] def isLeader: UIO[Boolean] = state.isLeader

  private[raft] def peers = state.peerList

  private def sendAppendEntries = for {
    currentTerm <- storage.getTerm
    peers       <- state.peerList
    commitIndex <- state.lastCommitIndex
    logSize     <- storage.logSize
    _ <- ZIO.collectAllPar(
      peers.map { p =>
        val appendProgram = for {
          nextIndex <- state.nextIndexForPeer(p).map(_.getOrElse(Index(0)))
          previousIndex = if (nextIndex == Index(-1)) nextIndex else nextIndex.decrement
          previousTerm <-
            storage.getEntry(previousIndex).map(_.map(_.term).getOrElse(Term(-1)))
          entries <- storage.getEntries(nextIndex)
          _ <- sendMessage(
            AppendEntries(
              AppendEntries.newUniqueId,
              nodeId,
              p,
              currentTerm,
              prevLogIndex = previousIndex,
              prevLogTerm = previousTerm,
              leaderCommitIndex = commitIndex,
              entries = entries
            )
          )
        } yield ()
        appendProgram
      }
    )
  } yield ()

  /**
   * If there exists an N such that N > commitIndex, a majority
   * of matchIndex[i] ≥ N, and log[N].term == currentTerm:
   * set commitIndex = N (§5.3, §5.4).
   */
  private def updateCommitIndex = for {
    term            <- storage.getTerm
    lastCommitIndex <- state.lastCommitIndex
    lastIndex       <- storage.lastIndex
    indices         <- state.matchIndexEntries.map(_.map { case (_, index) => index } :+ lastIndex)
    maybeMedian = indices.sortBy(_.index).lift(indices.size / 2)
    _ <- maybeMedian.fold[ZIO[Any, StorageException, Unit]](ZIO.unit) { median =>
      for {
        logEntry <- storage.getEntry(median)
        _ <- logEntry.fold[ZIO[Any, StorageException, Unit]](ZIO.unit) { entry =>
          ZIO.when(entry.term == term && median > lastCommitIndex)(state.updateCommitIndex(median))
        }
      } yield ()
    }
  } yield ()

  /**
   * AppendEntriesResponses can arrive out of order
   */
  private def handleAppendEntriesResponse(response: AppendEntriesResponse) =
    if (response.success)
      state.updateMatchIndex(response.from, response.lastInsertedIndex) *>
        state.updateNextIndex(response.from, response.lastInsertedIndex.increment) *>
        updateCommitIndex
    else state.decrementNextIndex(response.from)

  private def processAppendEntriesResponses = queues.appendResponseQueue.takeAll.flatMap { aers =>
    for {
      currentTerm <- storage.getTerm
      maxTerm = aers.map(_.term.term).maxOption
      _ <- maxTerm.fold[ZIO[Clock, StorageException, Unit]](IO.unit) { mx =>
        if (mx > currentTerm.term)
          (storage.storeTerm(Term(mx)) <*
            state.becomeFollower)
        else
          ZIO
            .collectAll(
              aers.map(response => handleAppendEntriesResponse(response))
            )
            .unit
      }
    } yield ()
  }.repeatWhileM(_ => isLeader)

  private def sendHeartbeats: ZIO[Clock, RaftException, Unit] = {

    lazy val sendHeartbeatEntries = sendAppendEntries
      .delay(Raft.LeaderHeartbeat)
      .repeatWhileM(_ => isLeader)

    (processAppendEntriesResponses &> sendHeartbeatEntries) *> processInboundEntries

  }

  private def sendVoteRequests(term: Term): IO[StorageException, Unit] = for {
    peers     <- state.peerList
    lastIndex <- storage.lastIndex
    lastTerm  <- storage.lastEntry.map(_.map(_.term).getOrElse(Term(-1)))
    requests = peers.map(p => VoteRequest(state.nodeId, p, term, lastIndex, lastTerm))
    _ <- sendMessages(requests)
  } yield ()

  private def collectVotes(forTerm: Term): ZIO[Clock, RaftException, Unit] = {
    lazy val voteCollectionProgram = for {
      newVote <- queues.inboundVoteResponseQueue.take
      processVote <-
        if (newVote.term.isAfter(forTerm))
          storage.storeTerm(newVote.term) *>
            state.becomeFollower
        else if (newVote.term == forTerm && newVote.granted)
          state.addVote(Vote(newVote.from, newVote.term)).flatMap { hasWon =>
            ZIO.when(hasWon)(storage.lastIndex.flatMap(state.initPeerIndices(_)))
          }
        else if (newVote.term == forTerm && !newVote.granted)
          state.addVoteRejection(Vote(newVote.from, newVote.term))
        else ZIO.unit
    } yield ()

    voteCollectionProgram
      .repeatUntilM(_ => nodeState.map(_ != NodeState.Candidate))
  }

  private def electionResult: ZIO[clock.Clock, RaftException, Unit] = for {
    state <- nodeState
    _ <- state match {
      case NodeState.Follower => processInboundEntries
      case NodeState.Candidate =>
        ZIO.die(InvalidStateException("Node still in Candidate state after election has concluded"))
      case NodeState.Leader => sendHeartbeats
    }
  } yield ()

  private[raft] def runForLeader = {

    val standForElectionProgram = for {
      newTerm <- storage.incrementTerm
      _       <- state.stand(newTerm)
      _       <- storage.storeVote(Vote(nodeId, newTerm))
      _       <- sendVoteRequests(newTerm)
    } yield newTerm

    standForElectionProgram.flatMap { term =>
      collectVotes(term) *> electionResult
    }
  }

  private def sendVoteResponse(voteRequest: VoteRequest, granted: Boolean) =
    sendMessage(VoteResponse(nodeId, voteRequest.from, voteRequest.term, granted))

  private def proccessVoteRequest(voteRequest: VoteRequest) = (for {
    currentTerm <- storage.getTerm
    currentVote <- storage.getVote
    lastIndex   <- storage.lastIndex
    lastEntry   <- storage.lastEntry
    _ <-
      if (
        lastIndex > voteRequest.lastLogIndex ||
        (lastIndex == voteRequest.lastLogIndex &&
          lastEntry.map(_.term).getOrElse(Term(-1)) > voteRequest.lastLogTerm)
      ) sendVoteResponse(voteRequest, granted = false)
      else if (voteRequest.term.isAfter(currentTerm))
        state.becomeFollower *>
          storage.storeTerm(voteRequest.term) *>
          storage.storeVote(Vote(voteRequest.from, voteRequest.term)) *>
          sendVoteResponse(voteRequest, granted = true)
      else if (
        voteRequest.term == currentTerm &&
        (currentVote.isEmpty || currentVote.contains(Vote(voteRequest.from, currentTerm)))
      )
        storage.storeVote(Vote(voteRequest.from, currentTerm)) *>
          sendVoteResponse(voteRequest, granted = true)
      else sendVoteResponse(voteRequest, granted = false)
  } yield ())

  private def processInboundVoteRequests: ZIO[Clock, RaftException, Unit] = queues.inboundVoteRequestQueue.take
    .flatMap(proccessVoteRequest)
    .forever

  private def shouldAppend(previousIndex: Index, previousTerm: Term) =
    if (previousIndex.index == -1) ZIO.succeed(true)
    else
      for {
        size <- storage.logSize
        term <- storage.getEntry(previousIndex).map(_.map(_.term).getOrElse(Term.Invalid))
        success = (previousTerm == term) && previousIndex.index == size - 1
        _ <- ZIO.when(!success)(storage.purgeFrom(previousIndex))
      } yield success

  private def appendLog(ae: AppendEntries) =
    shouldAppend(ae.prevLogIndex, ae.prevLogTerm).flatMap { shouldAppend =>
      ZIO.when(shouldAppend) {
        for {
          _             <- storage.appendEntries(ae.prevLogIndex.increment, ae.entries.toList)
          currentCommit <- state.lastCommitIndex
          _ <-
            ZIO.when(ae.leaderCommitIndex > currentCommit)(
              storage.logSize.flatMap { size =>
                val newCommitIndex = Index(math.min(size - 1, ae.leaderCommitIndex.index))
                state.updateCommitIndex(newCommitIndex) *>
                  storage.getRange(currentCommit.increment, newCommitIndex).flatMap { entries =>
                    ZIO.collectAll(
                      entries.map(e => stateMachine.write(e.command) *> state.incrementLastApplied)
                    )
                  }
              }
            )
        } yield ()
      }
    }

  private def processEntries(ae: AppendEntries) = (for {
    currentTerm  <- storage.getTerm
    currentState <- state.nodeState
    lastIndex    <- storage.logSize.map(x => Index(x - 1))
    result <-
      if (ae.term.isAfter(currentTerm))
        storage.storeTerm(ae.term) *>
          state.becomeFollower *>
          state.setLeader(ae.from) *>
          appendLog(ae).map(_ => (true, ae.term))
      else if (ae.term == currentTerm && currentState != NodeState.Follower)
        state.becomeFollower *>
          state.setLeader(ae.from) *>
          appendLog(ae).map(_ => (true, ae.term))
      else if (ae.term == currentTerm && currentState == NodeState.Follower)
        state.setLeader(ae.from) *>
          appendLog(ae).map(_ => (true, ae.term))
      else if (currentTerm.isAfter(ae.term)) ZIO.succeed((false, currentTerm))
      else ZIO.fail(InvalidStateException("Unknown state. This is likely a bug."))
    updatedLastIndex <- storage.logSize.map(x => Index(x - 1))
    (success, term) = result
    _ <- sendMessage(
      AppendEntriesResponse(
        ae.to,
        ae.from,
        ae.appendId,
        term,
        lastIndex,
        updatedLastIndex,
        success = success
      )
    )
  } yield ())

  private def processInboundEntries: ZIO[Clock, RaftException, Unit] =
    queues.appendEntriesQueue.take.disconnect
      .timeout(ElectionTimeout.newTimeout.millis.milliseconds)
      .flatMap { maybeEntries =>
        maybeEntries.fold[ZIO[Clock, RaftException, Unit]](
          for {
            nodeState   <- state.nodeState
            currentTerm <- storage.getTerm
            currentVote <- storage.getVote
            hasLost     <- state.hasLost(currentTerm)
            _ <-
              if ((nodeState == NodeState.Follower && currentVote.isEmpty) || hasLost) runForLeader
              else processInboundEntries
          } yield ()
        )(es => processEntries(es) *> processInboundEntries)
      }

  private[raft] def runFollowerLoop =
    processInboundVoteRequests <&> processInboundEntries

  def run = runFollowerLoop

  private def applyToStateMachine(command: WriteCommand) =
    stateMachine.write(command) *> state.incrementLastApplied

  private def processCommand(command: WriteCommand) = {
    lazy val processCommandProgram = for {
      term    <- storage.getTerm
      last    <- storage.lastIndex
      _       <- storage.appendEntry(last.increment, LogEntry(command, term))
      -       <- sendAppendEntries
      logSize <- storage.logSize
    } yield logSize

    processCommandProgram.flatMap { logSize =>
      state.lastCommitIndex
        .repeatUntil(_.index >= logSize - 1)
        .flatMap(_ => applyToStateMachine(command))
        .map(_ => Committed)
    }
  }

  /**
   * Blocks until it gets committed.
   */
  def submitCommand(command: WriteCommand): ZIO[Clock, RaftException, CommandResponse] =
    state.leader.flatMap { leader =>
      leader match {
        case Some(leaderId) if leaderId == nodeId => processCommand(command)
        case Some(leaderId) if leaderId != nodeId => ZIO.succeed(Redirect(leaderId))
        case None                                 => ZIO.succeed(LeaderNotFoundResponse)
      }
    }

  /**
   * Only the leader should allow reads. The leader needs to contact a quorum to verify it is still
   * the leader before returning.
   * This method exists for testing purposes.
   */
  private[raft] def submitQuery(query: ReadCommand): ZIO[Clock, RaftException, Option[T]] =
    stateMachine.read(query)

}

object Raft {

  case class MessageQueues(
    inboundVoteResponseQueue: Queue[VoteResponse],
    inboundVoteRequestQueue: Queue[VoteRequest],
    appendResponseQueue: Queue[AppendEntriesResponse],
    appendEntriesQueue: Queue[AppendEntries],
    outboundQueue: Queue[Message]
  )

  object MessageQueues {

    private val DefaultQueueSize = 100

    private def newQueue[T](queueSize: Int) =
      Queue.bounded[T](queueSize)

    def default = apply(DefaultQueueSize)

    def apply(queueSize: Int): UIO[MessageQueues] = for {
      inboundVoteResponseQueue <- newQueue[VoteResponse](queueSize)
      inboundVoteRequestQueue  <- newQueue[VoteRequest](queueSize)
      appendResponseQueue      <- newQueue[AppendEntriesResponse](queueSize)
      appendEntriesQueue       <- newQueue[AppendEntries](queueSize)
      outboundQueue            <- newQueue[Message](queueSize)
    } yield MessageQueues(
      inboundVoteResponseQueue,
      inboundVoteRequestQueue,
      appendResponseQueue,
      appendEntriesQueue,
      outboundQueue
    )
  }

  val LeaderHeartbeat = 50.milliseconds

  def default[T](storage: Storage, stateMachine: StateMachine[T]): UIO[Raft[T]] = {
    val id = NodeId.newUniqueId
    for {
      state  <- VolatileState(id, Set.empty[NodeId])
      queues <- MessageQueues.default
    } yield new Raft[T](id, state, storage, queues, stateMachine)
  }

  def apply[T](
    nodeId: NodeId,
    peers: Set[NodeId],
    storage: Storage,
    stateMachine: StateMachine[T]
  ) = for {
    state  <- VolatileState(nodeId, peers)
    queues <- MessageQueues.default
  } yield new Raft[T](nodeId, state, storage, queues, stateMachine)

}
