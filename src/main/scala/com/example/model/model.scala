package com.example.model

import at.favre.lib.bytes.Bytes

final case class Gid(bytes: Bytes)

final case class Topic(name: String) extends AnyVal

final case class SqsQueueUrl(str: String) extends AnyVal

final case class SqsMessage(topic: String, content: String)

final case class MetricsPrefix(str: String) extends AnyVal

final case class ContainerId(str: String) extends AnyVal

final case class Beacon(
    remote_addr: String,
    http_x_forwarded_for: String,
    time_iso8601: String,
    request_method: String,
    server_protocol: String,
    uid_set: String,
    uid_got: String,
    request_body: String,
    http_referer: String,
    http_user_agent: String,
    trace_id: String,
    status: String = "-",
    body_bytes_sent: String = "-"
  )
