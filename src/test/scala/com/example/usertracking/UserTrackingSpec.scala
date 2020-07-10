package com.example.usertracking

import at.favre.lib.bytes.Bytes
import cats.syntax.parallel._
import com.example.model.Gid
import com.example.usertracking.GidSyntax._
import monix.execution.Scheduler.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class UserTrackingSpec extends AnyFlatSpecLike with Matchers {

  "UserTracking.GenerateTrackingId" should "generate different 128 bit id every time its called" in {
    val firstGid = GenerateTrackingId()
    val secondGid = GenerateTrackingId()

    (firstGid, secondGid)
      .parMapN {
        case (gidOne, gidTwo) =>
          gidOne.bytes.array should have size 16
          gidTwo.bytes.array should have size 16
          gidOne should not be gidTwo
      }
      .runSyncUnsafe()
  }

  "UserTracking.GidOps" should "encode gid in hex and base64 format" in {
    Gid(Bytes.parseBase64("CgAxSV6OOhHCymK2NhUhAg==")).asBase64 shouldBe "CgAxSV6OOhHCymK2NhUhAg=="
    Gid(Bytes.parseBase64("CgAxSV6OOhHCymK2NhUhAg==")).asHex shouldBe "4931000A113A8E5EB662CAC202211536"
  }
}
