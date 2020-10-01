package com.ariskk.raft.model

import com.ariskk.raft.utils.Utils

final case class NodeId(value: String) extends AnyVal

object NodeId {
  def newUniqueId: NodeId = NodeId(Utils.newPrefixedId("node"))
}
