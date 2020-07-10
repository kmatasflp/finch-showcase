package com.example

import java.net.InetAddress
import java.time._
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import _root_.io.circe.generic.auto._
import _root_.io.circe.syntax._
import _root_.io.finch._
import at.favre.lib.bytes.Bytes
import cats.syntax.functor._
import cats.syntax.apply._
import com.example.io.Storage
import com.example.model.{ Beacon, Gid }
import com.example.monitoring.Metrics
import com.example.usertracking.GenerateTrackingId
import com.example.usertracking.GidSyntax._
import com.twitter.finagle.http.Cookie
import com.twitter.finagle.http.cookie.SameSite
import com.twitter.util.Duration
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import retry.{ RetryDetails, RetryPolicies }
import retry.syntax.all._

import scala.concurrent.duration._

trait Api extends Endpoint.Module[Task] with StrictLogging with Metrics {

  private val sqsPublishingRetryRate = metrics.meter("sqs-publishing-retry-rate")
  private val kafkaPublishingFailureRate = metrics.meter("kafka-publishing-failure-rate")

  protected def kafkaStorage: Storage
  protected def sqsStorage: Storage
  protected def currentDateTime: ZonedDateTime
  protected def createTraceId: String

  protected def domain: String

  private val isoDateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX")
  private val unset = "-"
  private val retryPolicy = RetryPolicies.fullJitter[Task](30.seconds)

  def healthCheck: Endpoint[Task, String] = get("health-check") {
    Task.now(Ok("healthy"))
  }

  def ingest: Endpoint[Task, Unit] =
    post(
      "beacons" :: stringBody :: cookieOption("gid") :: root.map(_.remoteAddress) :: root
        .map(_.xForwardedFor) :: root.map(_.referer) :: root.map(_.userAgent)
    ) {
      (
          body: String,
          maybeGidCookie: Option[Cookie],
          remoteAddress: InetAddress,
          maybeXForwardedFor: Option[String],
          maybeReferer: Option[String],
          maybeUserAgent: Option[String]
      ) =>
        val message =
          Beacon(
            remote_addr = remoteAddress.getHostAddress(),
            http_x_forwarded_for = maybeXForwardedFor.getOrElse(unset),
            time_iso8601 = currentDateTime.format(isoDateTimeFormat),
            request_method = "POST",
            server_protocol = "HTTP/1.1",
            uid_set = unset,
            uid_got = unset,
            request_body = body,
            http_referer = maybeReferer.getOrElse(unset),
            http_user_agent = maybeUserAgent.getOrElse(unset),
            trace_id = s"kafka-ingestor-${createTraceId}"
          )

        maybeGidCookie
          .fold(
            GenerateTrackingId().flatMap { gid =>
              storeMessage(message.copy(uid_set = gid.asHex))
                .as(NoContent[Unit].withCookie(createUserTrackingCookie(gid)))
            }
          ) { gidCookie =>
            val gid = Gid(Bytes.parseBase64(gidCookie.value))
            storeMessage(message.copy(uid_got = gid.asHex)).as(NoContent[Unit])
          }
    }.handle {
      case t =>
        logger.error("Unexpected error occured", t)
        InternalServerError(new IllegalStateException("Something bad happened"))
    }

  private def storeMessage(m: Beacon) = {

    def logRetry[A](r: A, details: RetryDetails) =
      (
        Task.now(sqsPublishingRetryRate.mark()),
        Task(logger.warn(s"Retrying, got $r, details $details"))
      ).mapN((_, _)).void

    val strMessage = m.asJson.noSpaces

    kafkaStorage.storeBeacon(strMessage).onErrorRecoverWith {
      case e =>
        logger.error(s"Failed to store content to kafka, $strMessage, sending it to SQS", e)
        kafkaPublishingFailureRate.mark()

        sqsStorage.storeBeacon(strMessage).retryingOnAllErrors(retryPolicy, logRetry)
    }
  }

  private def createUserTrackingCookie(gid: Gid) =
    new Cookie(
      name = "gid",
      value = gid.asBase64,
      domain = Some(domain),
      path = Some("/"),
      maxAge = Some(Duration(730, TimeUnit.DAYS)),
      secure = true,
      sameSite = SameSite.None
    )

}

object LiveApi {
  def apply(
      _kafkaStorage: Storage,
      _sqsStorage: Storage,
      _domain: String
    ): Api =
    new Api {
      override val kafkaStorage: Storage = _kafkaStorage
      override val sqsStorage: Storage = _sqsStorage
      override val domain: String = _domain
      override def currentDateTime: ZonedDateTime = ZonedDateTime.now(ZoneId.of("UTC-4"))
      override def createTraceId: String =
        Bytes.random(20).encodeHex

    }
}
