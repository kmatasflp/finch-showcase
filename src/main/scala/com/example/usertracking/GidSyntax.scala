package com.example.usertracking

import java.nio.ByteOrder

import com.example.model.Gid

object GidSyntax {
  implicit class GidOps(gid: Gid) {
    def asBase64: String =
      gid.bytes.encodeBase64()

    def asHex: String = {
      val bb = gid.bytes.byteOrder(ByteOrder.LITTLE_ENDIAN).buffer()
      String.format("%08X%08X%08X%08X", bb.getInt(), bb.getInt(), bb.getInt(), bb.getInt())
    }
  }
}
