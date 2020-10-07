# zio-raft

An Implementation of Raft using ZIO in Scala.

## Raft

Raft is a popular consensus algorithm. It solves the problem of multiple servers needing to agree on a single value.
It is a coordination primitive often used in databases and other distributed systems.
For more info, check out https://raft.github.io/

This is a work in progress and likely contains a ton of very nasty bugs. What is more, 
I am new to ZIO and it is very likely there are nicer, more idiomatic ways to express computations.
Feedback always welcome!

## Implementation

TBD

Missing:
- Apply commands to the state machine
- Safety - 5.4 and onwards (see https://raft.github.io/raft.pdf)
- Memberships
- RPC. Currently network is emulated in `TestCluster` by introducing non-Byzantine failures to message passing.
- Persistent storage. Likely RocksDB
- Proven Linearizability (ie using Jepsen)
