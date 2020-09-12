package com.ariskk.raft.model

case class Vote(peerId: RaftNode.Id, term: Term)
