# zio-raft

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/ariskk/zio-raft/blob/master/LICENSE)

An Implementation of Raft using ZIO in Scala.

## Raft

Raft is a popular consensus algorithm. It solves the problem of multiple servers needing to agree on a single value.
It is a coordination primitive often used in databases and other distributed systems.
For more info, check out the paper https://raft.github.io/

This is a work in progress implementation in Scala using ZIO for effect management. `Log`, `Storage` and `StateMachine` are pluggable and 
thus different implementations can be provided. The repo currently provides a `Storage` implementation using [RocksDB](https://rocksdb.org/).

[`Raft`](https://github.com/ariskk/zio-raft/blob/master/core/src/main/scala/com.ariskk.raft/Raft.scala) contains the core implementation of the consensus algorithm.
The implementation is fully asynchronous. The consensus module is decoupled from the RPC layer and communicates with the outside world using Queues. 
[`RaftServer`](https://github.com/ariskk/zio-raft/blob/master/server/src/main/scala/com.ariskk.raft.server/RaftServer.scala) contains a reference implementation of RPC 
using a lightweight ZIO wrapper around Java's NIO.

For examples on how to use the `RaftServer`, please refer to [`RaftServerSpec`](https://github.com/ariskk/zio-raft/blob/master/server/src/test/scala/com.ariskk.raft.server/RaftServerSpec.scala)
