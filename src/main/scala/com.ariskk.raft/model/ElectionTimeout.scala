package com.ariskk.raft.model

import scala.util.Random

final case class ElectionTimeout(value: Int) extends AnyVal

object ElectionTimeout {
  private val base = 150
  private val range = 150

  def newTimeout: ElectionTimeout =
    ElectionTimeout(base + Random.nextInt(range))
}