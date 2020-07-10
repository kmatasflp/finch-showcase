package com.example.usertracking

import at.favre.lib.bytes.Bytes
import cats.effect.concurrent.Ref
import com.example.model.Gid
import monix.eval.Task

trait GenerateTrackingId extends (() => Task[Gid]) {

  def seed: Bytes

  private val counter = Ref.of[Task, Int](0).memoizeOnSuccess

  def apply(): Task[Gid] =
    for {
      ref <- counter
      c <- ref.modify(x => (x + 1, x))
      t = System.nanoTime()
    } yield {
      Gid(Bytes.empty.append(seed).append(t).append(c).copy())
    }
}

object GenerateTrackingId extends GenerateTrackingId {
  val seed: Bytes = Bytes.random(4)
}
