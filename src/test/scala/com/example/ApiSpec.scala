package com.example

import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

import _root_.io.circe.generic.auto._
import _root_.io.circe.Json
import _root_.io.circe.parser._
import _root_.io.circe.syntax._
import _root_.io.finch.{ Input, NoContent, Ok }
import at.favre.lib.bytes.Bytes
import com.example.Api
import com.example.io.{ Storage, StorageFailure }
import com.example.model.{ Beacon, Gid }
import com.example.usertracking.GidSyntax._
import com.twitter.finagle.http.Cookie
import com.twitter.finagle.http.cookie.SameSite
import com.twitter.util.Duration
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues
import org.scalactic.Equality

class ApiSpec extends AnyFlatSpecLike with Matchers with OptionValues {

  private val someFixedDateTime = "2020-04-21T13:41:37-04:00"
  private val someDomain = "test.example.com"
  private val someFixedTraceId = "a9b16cc8a9f148226016a271177b8b3166a6bc5e"

  implicit private val cookieEq: Equality[Cookie] = (`this`: Cookie, o: Any) =>
    o match {
      case that: Cookie =>
        `this`.name == that.name &&
          `this`.value.nonEmpty &&
          `this`.domain == that.domain &&
          `this`.path == that.path &&
          `this`.maxAge == that.maxAge &&
          `this`.secure == that.secure &&
          `this`.sameSite == that.sameSite
      case _ => false
    }

  "Api" should "respond with NoContent and store requests in kafka" in withApi {
    (api, kafka, sqs) =>
      val gidCookie = "gid=CgAxSV6OOhHCymK2NhUhAg=="

      val body =
        "%5B%7B%22base%22%3A%7B%22aid%22%3A%22Hosted%22%2C%22postalCode%22%3A%7B%22string%22%3A%220000%22%7D%2C%22rnd%22%3A%220.19448658739243352%22%7D%2C%22flyer%22%3A%7B%22flyerId%22%3A1904390%2C%22flyerRunId%22%3A317077%2C%22flyerTypeId%22%3A489%2C%22premium%22%3Atrue%7D%2C%22flyerItem%22%3A%20%7B%22id%22%3A%20303222720%7D%2C%22hostedBase%22%3A%7B%22browser%22%3A%22Mobile%20Safari%22%2C%22browserVersion%22%3A%2211.0%22%2C%22devicePixelRatio%22%3A3%2C%22host%22%3A%22http%3A%2F%2Flocalhost%3A8080%2Fiframe.html%22%2C%22iframeWidth%22%3A414%2C%22screenHeight%22%3A736%2C%22screenWidth%22%3A414%2C%22system%22%3A%22iOS%22%2C%22systemModel%22%3A%22Apple%22%2C%22systemVersion%22%3A%2211.0%22%2C%22cookieId%22%3A%22some-cookie-id%22%2C%22environment%22%3A%22Prod%22%2C%22epochMilliseconds%22%3A1536947040000%2C%22globalCookieId%22%3A%7B%22string%22%3A%22some-global-cookie-id%22%7D%2C%22sequenceId%22%3A2%2C%22sessionId%22%3A%22some-session-id%22%2C%22storeCode%22%3A%7B%22string%22%3A%222648%22%7D%2C%22uuid%22%3A%22some-uuid%22%7D%2C%22merchant%22%3A%7B%22merchantId%22%3A2175%7D%2C%22schemaInfo%22%3A%7B%22schemaId%22%3A1%7D%2C%22storefront%22%3A%7B%22sfmlUUID%22%3A%22some-other-uuid%22%7D%7D%5D"

      api
        .ingest(
          Input
            .post("/beacons")
            .withHeaders("User-Agent" -> "some-user-agent")
            .withHeaders("X-Forwarded-For" -> "129.78.138.66, 129.78.64.103")
            .withHeaders("Cookie" -> gidCookie)
            .withHeaders("Referer" -> "https://example.com/referred")
            .withBody(
              body
            )
        )
        .awaitOutputUnsafe() shouldBe Some(NoContent)

      kafka
        .storedBeacons
        .map(m => parse(m).getOrElse(Json.Null)) should contain theSameElementsAs List(
        Beacon(
          remote_addr = "0.0.0.0",
          http_x_forwarded_for = "129.78.138.66, 129.78.64.103",
          time_iso8601 = someFixedDateTime,
          request_method = "POST",
          server_protocol = "HTTP/1.1",
          uid_set = "-",
          uid_got = "4931000A113A8E5EB662CAC202211536",
          request_body = body,
          http_referer = "https://example.com/referred",
          http_user_agent = "some-user-agent",
          trace_id = s"kafka-ingestor-${someFixedTraceId}"
        ).asJson
      )

      sqs.storedBeacons shouldBe empty
  }

  it should "generate `gid` cookie if request does not contain it" in withApi { (api, kafka, _) =>
    val request = Input
      .post("/beacons")
      .withBody(
        "some-body"
      )

    val response = api.ingest(request).awaitOutputUnsafe().value

    val receivedCookie = response.cookies.find(_.name == "gid").value

    val expectedCookie = new Cookie(
      name = "gid",
      value = "some-value",
      domain = Some(someDomain),
      path = Some("/"),
      maxAge = Some(Duration(730, TimeUnit.DAYS)),
      secure = true,
      sameSite = SameSite.None
    )

    receivedCookie shouldEqual expectedCookie

    val storedGidHex = kafka
      .storedBeacons
      .flatMap(b => decode[Beacon](b).fold(_ => List.empty[Beacon], m => List(m)))
      .map(_.uid_set)
      .head

    val gotGidB64 = receivedCookie.value

    storedGidHex shouldBe Gid(Bytes.parseBase64(gotGidB64)).asHex
  }

  it should "should not generate `gid` cookie if it is provided with request" in withApi {
    (api, kafka, _) =>
      val gidCookie = "gid=CgAxSV6OOhHCymK2NhUhAg=="

      val request = Input
        .post("/beacons")
        .withHeaders("Cookie" -> gidCookie)
        .withBody(
          "some-body"
        )

      val response = api.ingest(request).awaitOutputUnsafe().value

      val storedGidHex = kafka
        .storedBeacons
        .flatMap(b => decode[Beacon](b).fold(_ => List.empty[Beacon], m => List(m)))
        .map(_.uid_got)
        .head

      storedGidHex shouldBe "4931000A113A8E5EB662CAC202211536"

      response.cookies.map(_.name) should not contain ("gid")
  }

  it should "store messages in sqs in case kafka is unavailable" in withApi(
    { (api, kafka, sqs) =>
      val gidCookie = "gid=CgAxSV6OOhHCymK2NhUhAg=="

      api
        .ingest(
          Input
            .post("/beacons")
            .withHeaders("User-Agent" -> "some-user-agent")
            .withHeaders("X-Forwarded-For" -> "129.78.138.66, 129.78.64.103")
            .withHeaders("Cookie" -> gidCookie)
            .withHeaders("Referer" -> "https://example.com/referred")
            .withBody(
              "some-body"
            )
        )
        .awaitOutputUnsafe() shouldBe Some(NoContent)

      kafka.storedBeacons shouldBe empty

      sqs
        .storedBeacons
        .map(m => parse(m).getOrElse(Json.Null)) should contain theSameElementsAs List(
        Beacon(
          remote_addr = "0.0.0.0",
          http_x_forwarded_for = "129.78.138.66, 129.78.64.103",
          time_iso8601 = someFixedDateTime,
          request_method = "POST",
          server_protocol = "HTTP/1.1",
          uid_set = "-",
          uid_got = "4931000A113A8E5EB662CAC202211536",
          request_body = "some-body",
          http_referer = "https://example.com/referred",
          http_user_agent = "some-user-agent",
          trace_id = s"kafka-ingestor-${someFixedTraceId}"
        ).asJson
      )

    },
    kafkaStorageFailure = Some(new IllegalStateException("Boom"))
  )

  it should "respond `healthy` in case health-check endpoint is called" in withApi { (api, _, _) =>
    api.healthCheck(Input.get("/health-check")).awaitOutputUnsafe() shouldBe Some(Ok("healthy"))
  }

  private def withApi(
      testCode: (Api, VectorStorage, VectorStorage) => Any,
      kafkaStorageFailure: Option[Exception] = None
    ) = {

    val kafkaVectorStorage = new VectorStorage(kafkaStorageFailure)
    val sqsVectorStorage = new VectorStorage

    val api = new Api {
      override val kafkaStorage: Storage = kafkaVectorStorage
      override val sqsStorage: Storage = sqsVectorStorage
      override val domain: String = someDomain
      override def currentDateTime: ZonedDateTime =
        ZonedDateTime.parse(someFixedDateTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
      override def createTraceId: String = someFixedTraceId
    }

    testCode(api, kafkaVectorStorage, sqsVectorStorage)
  }

  private class VectorStorage(val failure: Option[Exception] = None) extends Storage {
    private val storedBeaconsBldr = Vector.newBuilder[String]

    override def storeBeacon(what: String): Task[Unit] =
      failure.fold(Task[Unit](storedBeaconsBldr += what)) {
        case e: Exception => Task.raiseError(StorageFailure(e))
        case t            => Task.raiseError(t)
      }

    def storedBeacons: Vector[String] = storedBeaconsBldr.result()
  }

}
