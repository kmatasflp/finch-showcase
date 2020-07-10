package com.example

import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

import _root_.io.circe.generic.auto._
import _root_.io.circe.parser._
import _root_.io.circe.syntax._
import cats.effect.ExitCode
import com.codahale.metrics.{ MetricFilter, SharedMetricRegistries }
import com.example.fixtures.RunningKafka
import com.example.model._
import com.twitter.finagle.http._
import com.twitter.finagle.http.cookie.SameSite
import com.twitter.io.Buf
import com.twitter.util.Duration
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.Codecs.stringKeyValueCrDecoder
import net.manub.embeddedkafka.EmbeddedKafka
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.serialization._
import org.elasticmq.rest.sqs.SQSRestServerBuilder
import org.scalactic.Equality
import org.scalatest._
import org.scalatest.compatible.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.FixtureAnyFeatureSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.model._
import software.amazon.awssdk.services.sqs.model.Message
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import scala.jdk.CollectionConverters._
import scala.util.Try

class KafkaIngestionServerSpec
    extends FixtureAnyFeatureSpecLike
    with ServiceIntegrationSuite
    with RunningKafka
    with GivenWhenThen
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Eventually
    with OptionValues
    with Matchers {

  private val sqsInterface = "localhost"
  private val sqsPort = 9324

  private val sqsServer = SQSRestServerBuilder
    .withPort(sqsPort)
    .withInterface(sqsInterface)
    .withAWSRegion(Region.US_EAST_1.id())
    .start()

  private val sqsClient = SqsAsyncClient
    .builder
    .region(Region.US_EAST_1)
    .endpointOverride(URI.create(s"http://$sqsInterface:$sqsPort"))
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
    )
    .build

  implicit private val stringDeserializer: Deserializer[String] = new StringDeserializer()

  private val outputTopic = Topic("out")

  implicit private val beaconEq: Equality[Beacon] = (`this`: Beacon, o: Any) =>
    o match {
      case that: Beacon =>
        `this`.remote_addr == that.remote_addr &&
          `this`.http_x_forwarded_for == that.http_x_forwarded_for &&
          Try(ZonedDateTime.parse(`this`.time_iso8601, DateTimeFormatter.ISO_OFFSET_DATE_TIME)).isSuccess &&
          `this`.request_method == that.request_method &&
          `this`.server_protocol == that.server_protocol &&
          `this`.uid_got == that.uid_got &&
          `this`.uid_set == that.uid_set &&
          `this`.request_body == that.request_body &&
          `this`.http_referer == that.http_referer &&
          `this`.http_user_agent == that.http_user_agent &&
          `this`.trace_id.startsWith("kafka-ingestor-") &&
          `this`.status == that.status &&
          `this`.body_bytes_sent == that.body_bytes_sent
      case _ => false
    }

  implicit private val sqsMessageEq: Equality[SqsMessage] = (`this`: SqsMessage, o: Any) =>
    o match {
      case that: SqsMessage =>
        val thisContent = decode[Beacon](`this`.content).getOrElse(null)
        val thatContent = decode[Beacon](that.content).getOrElse(null)

        `this`.topic == that.topic && beaconEq.areEqual(thisContent, thatContent)
      case _ => false
    }

  override implicit val embeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = 8002, zooKeeperPort = 8003)

  private val sqsQueue = createQueue("some-queue")

  override def port: Int = KafkaIngestionServer.port

  private val someDomain = "test.example.com"

  override def createService(): Task[ExitCode] = {

    System.setProperty("KAFKA_PRODUCER_MAX_BLOCK_MS", "1000")

    System.setProperty("aws.accessKeyId", "x")
    System.setProperty("aws.secretAccessKey", "x")

    // format: off
    KafkaIngestionServer.run(List(
      "--outputTopic", outputTopic.name,
      "--sqsQueueUrl", sqsQueue.str,
      "--metricsEndpointHost", "localhost",
      "--metricsEndpointPort", "8125",
      "--containerId", "some-container-id",
      "--awsRegion", "us-east-1",
      "--awsSqsEndpoint", "http://localhost:9324",
      "--kafkaBootstrapServers", s"localhost:${embeddedKafkaConfig.kafkaPort}",
      "--domain", someDomain
    ))
    // format: on
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.createCustomTopic(outputTopic.name)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sqsClient.close()
    sqsServer.stopAndWait()
  }

  override protected def beforeEach(): Unit =
    SharedMetricRegistries.getOrCreate("default").removeMatching(MetricFilter.ALL)

  implicit private val cookieEq: Equality[Cookie] = (`this`: Cookie, o: Any) =>
    o match {
      case that: Cookie =>
        `this`.name == that.name &&
          `this`.value.nonEmpty &&
          `this`.domain == that.domain &&
          `this`.path == that.path &&
          Duration(730, TimeUnit.DAYS).minus(`this`.maxAge) <= (Duration(5, TimeUnit.MINUTES)) &&
          `this`.secure == that.secure &&
          `this`.sameSite == that.sameSite
      case _ => false
    }

  Feature("Store incoming beacons") {
    Scenario(
      "service should respond with NoContent and store requests hitting /beacons route in Kafka"
    ) { client =>
      Given("service receives POST request on /beacons route")
      val requestBody =
        "%5B%7B%22base%22%3A%7B%22aid%22%3A%22Hosted%22%2C%22postalCode%22%3A%7B%22string%22%3A%220000%22%7D%2C%22rnd%22%3A%220.19448658739243352%22%7D%2C%22flyer%22%3A%7B%22flyerId%22%3A1904390%2C%22flyerRunId%22%3A317077%2C%22flyerTypeId%22%3A489%2C%22premium%22%3Atrue%7D%2C%22flyerItem%22%3A%20%7B%22id%22%3A%20303222720%7D%2C%22hostedBase%22%3A%7B%22browser%22%3A%22Mobile%20Safari%22%2C%22browserVersion%22%3A%2211.0%22%2C%22devicePixelRatio%22%3A3%2C%22host%22%3A%22http%3A%2F%2Flocalhost%3A8080%2Fiframe.html%22%2C%22iframeWidth%22%3A414%2C%22screenHeight%22%3A736%2C%22screenWidth%22%3A414%2C%22system%22%3A%22iOS%22%2C%22systemModel%22%3A%22Apple%22%2C%22systemVersion%22%3A%2211.0%22%2C%22cookieId%22%3A%22some-cookie-id%22%2C%22environment%22%3A%22Prod%22%2C%22epochMilliseconds%22%3A1536947040000%2C%22globalCookieId%22%3A%7B%22string%22%3A%22some-global-cookie-id%22%7D%2C%22sequenceId%22%3A2%2C%22sessionId%22%3A%22some-session-id%22%2C%22storeCode%22%3A%7B%22string%22%3A%222648%22%7D%2C%22uuid%22%3A%22some-uuid%22%7D%2C%22merchant%22%3A%7B%22merchantId%22%3A2175%7D%2C%22schemaInfo%22%3A%7B%22schemaId%22%3A1%7D%2C%22storefront%22%3A%7B%22sfmlUUID%22%3A%22some-other-uuid%22%7D%7D%5D"

      val request = Request(Method.Post, "/beacons")

      request.content(Buf.Utf8(requestBody))
      request.userAgent_=("some-user-agent")
      request.xForwardedFor_=("129.78.138.66, 129.78.64.103")
      request.referer_=("https://example.com/referred")

      val gidCookie = "gid=CgAxSV6OOhHCymK2NhUhAg=="
      request.headerMap.add("Cookie", gidCookie)

      request.headerMap.add("Origin", "http://example.com")

      val response = client(request)

      Then("response should be NoContent")
      response.status shouldBe Status.NoContent

      And("Cross-Origin Resource Sharing (CORS) headers are returned")
      response.headerMap.iterator.toList should contain.allOf(
        "Access-Control-Allow-Origin" -> "http://example.com",
        "Access-Control-Allow-Credentials" -> "true"
      )

      And("output topic should contain stored request")
      EmbeddedKafka.withConsumer[String, String, Assertion] { consumer =>
        val value = consumer.consumeLazily[(String, String)](outputTopic.name).last._2
        val storedMessage: Beacon = decode[Beacon](value).getOrElse(null)

        storedMessage shouldEqual Beacon(
          remote_addr = "127.0.0.1",
          http_x_forwarded_for = "129.78.138.66, 129.78.64.103",
          time_iso8601 = "some-iso-datetime",
          request_method = "POST",
          server_protocol = "HTTP/1.1",
          uid_set = "-",
          uid_got = "4931000A113A8E5EB662CAC202211536",
          request_body = requestBody,
          http_referer = "https://example.com/referred",
          http_user_agent = "some-user-agent",
          trace_id = "some-trace-id",
          status = "-",
          body_bytes_sent = "-"
        )
      }

      And("Metrics have been reported")
      withClue("Total incoming requests count") {
        getMeter("incoming_requests.total").getCount shouldBe 1
      }

      withClue("Beacons incoming requests count") {
        getMeter("/beacons.incoming_requests").getCount shouldBe 1
      }

      withClue("Beacons successful response count") {
        getMeter("/beacons.successes").getCount shouldBe 1
      }

      withClue("Beacons processing response time count") {
        getHistogram("/beacons.response_time").getCount shouldBe 1
      }

    }

    Scenario(
      "service should generate `gid` cookie if request does not contain it"
    ) { client =>
      Given("service receives POST request on /beacons route")
      val requestBody = "some-body"

      val request = Request(Method.Post, "/beacons")

      request.content(Buf.Utf8(requestBody))
      request.userAgent_=("some-user-agent")
      request.xForwardedFor_=("129.78.138.66, 129.78.64.103")
      request.referer_=("https://example.com/referred")

      request.headerMap.add("Origin", "http://example.com")

      val response = client(request)

      Then("response should be NoContent")
      response.status shouldBe Status.NoContent

      And("gid cookie is returned")
      val receivedGidCookie = response.cookies.get("gid").value

      val expectedGidCookie = new Cookie(
        name = "gid",
        value = "some-value",
        domain = Some(someDomain),
        path = Some("/"),
        maxAge = Some(Duration(730, TimeUnit.DAYS)),
        secure = true,
        sameSite = SameSite.None
      )

      receivedGidCookie shouldEqual expectedGidCookie

    }

    Scenario("service should support Cross-Origin Resource Sharing (CORS) preflighted requests") {
      client =>
        val request = Request(Method.Options, "/beacons")

        request.headerMap.add("Origin", "http://example.com")
        request.headerMap.add("Access-Control-Request-Method", "POST")
        request
          .headerMap
          .add(
            "Access-Control-Request-Headers",
            "Content-Type, Referer, User-Agent, X-Requested-With"
          )

        val response = client(request)

        response.status shouldBe Status.Ok
        response.headerMap.iterator.toList should contain.allOf(
          "Access-Control-Allow-Origin" -> "http://example.com",
          "Access-Control-Allow-Methods" -> "GET, POST",
          "Access-Control-Allow-Headers" -> "Content-Type, Referer, User-Agent, X-Requested-With",
          "Access-Control-Allow-Credentials" -> "true",
          "Access-Control-Max-Age" -> "86400",
          "Vary" -> "Origin"
        )

    }

    Scenario(
      "service should respond with NoContent and store requests hitting /beacons route in SQS in case Kafka is unavailable"
    ) { client =>
      Given("service receives POST request on /beacons route")
      val requestBody =
        "%5B%7B%22base%22%3A%7B%22aid%22%3A%22Hosted%22%2C%22postalCode%22%3A%7B%22string%22%3A%220000%22%7D%2C%22rnd%22%3A%220.19448658739243352%22%7D%2C%22flyer%22%3A%7B%22flyerId%22%3A1904390%2C%22flyerRunId%22%3A317077%2C%22flyerTypeId%22%3A489%2C%22premium%22%3Atrue%7D%2C%22flyerItem%22%3A%20%7B%22id%22%3A%20303222720%7D%2C%22hostedBase%22%3A%7B%22browser%22%3A%22Mobile%20Safari%22%2C%22browserVersion%22%3A%2211.0%22%2C%22devicePixelRatio%22%3A3%2C%22host%22%3A%22http%3A%2F%2Flocalhost%3A8080%2Fiframe.html%22%2C%22iframeWidth%22%3A414%2C%22screenHeight%22%3A736%2C%22screenWidth%22%3A414%2C%22system%22%3A%22iOS%22%2C%22systemModel%22%3A%22Apple%22%2C%22systemVersion%22%3A%2211.0%22%2C%22cookieId%22%3A%22some-cookie-id%22%2C%22environment%22%3A%22Prod%22%2C%22epochMilliseconds%22%3A1536947040000%2C%22globalCookieId%22%3A%7B%22string%22%3A%22some-global-cookie-id%22%7D%2C%22sequenceId%22%3A2%2C%22sessionId%22%3A%22some-session-id%22%2C%22storeCode%22%3A%7B%22string%22%3A%222648%22%7D%2C%22uuid%22%3A%22some-uuid%22%7D%2C%22merchant%22%3A%7B%22merchantId%22%3A2175%7D%2C%22schemaInfo%22%3A%7B%22schemaId%22%3A1%7D%2C%22storefront%22%3A%7B%22sfmlUUID%22%3A%22some-other-uuid%22%7D%7D%5D"

      val request = Request(Method.Post, "/beacons")

      request.content(Buf.Utf8(requestBody))
      request.userAgent_=("some-user-agent")
      request.xForwardedFor_=("129.78.138.66, 129.78.64.103")
      request.referer_=("https://example.com/referred")

      val gidCookie = "gid=CgAxSV6OOhHCymK2NhUhAg=="
      request.headerMap.add("Cookie", gidCookie)

      EmbeddedKafka.stop()

      val response =
        try {
          client(request)
        }
        finally {
          EmbeddedKafka.start()
        }

      Then("response should be NoContent")
      response.status shouldBe Status.NoContent

      And("sqs should contain stored request")
      consumeMessagesFrom(sqsQueue) should contain theSameElementsAs List(
        SqsMessage(
          outputTopic.name,
          content = Beacon(
            remote_addr = "127.0.0.1",
            http_x_forwarded_for = "129.78.138.66, 129.78.64.103",
            time_iso8601 = "some-iso-datetime",
            request_method = "POST",
            server_protocol = "HTTP/1.1",
            uid_set = "-",
            uid_got = "4931000A113A8E5EB662CAC202211536",
            request_body = requestBody,
            http_referer = "https://example.com/referred",
            http_user_agent = "some-user-agent",
            trace_id = "some-trace-id",
            status = "-",
            body_bytes_sent = "-"
          ).asJson.noSpaces
        )
      )

      And("Metrics have been reported")
      withClue("Total incoming requests count") {
        getMeter("incoming_requests.total").getCount shouldBe 1
      }

      withClue("Beacons incoming requests count") {
        getMeter("/beacons.incoming_requests").getCount shouldBe 1
      }

      withClue("Beacons successful response count") {
        getMeter("/beacons.successes").getCount shouldBe 1
      }

      withClue("Publishing to Kafka failures count") {
        getMeter("kafka-publishing-failure-rate").getCount shouldBe 1
      }

      withClue("Beacons processing response time count") {
        getHistogram("/beacons.response_time").getCount shouldBe 1
      }
    }

    Scenario("service should return 'healthy' in case /health-check endpoint is called") { client =>
      val request = Request(Method.Get, "/health-check")

      val response = client(request)

      response.status shouldBe Status.Ok
      response.contentString shouldBe "healthy"
    }
  }

  private def getMeter(name: String) = {
    val (_, meter) =
      SharedMetricRegistries
        .getOrCreate("default")
        .getMeters(MetricFilter.contains(name))
        .asScala
        .headOption
        .value
    meter
  }

  private def getHistogram(name: String) = {
    val (_, histogram) =
      SharedMetricRegistries
        .getOrCreate("default")
        .getHistograms(MetricFilter.contains(name))
        .asScala
        .headOption
        .value
    histogram
  }

  private def createQueue(name: String) = {
    val req = CreateQueueRequest.builder.queueName(name).build

    val queueUrl = Task
      .async[CreateQueueResponse] { cb =>
        sqsClient
          .createQueue(req)
          .whenComplete { (res, ex) =>
            if (ex != null)
              cb(Left(ex))
            else
              cb(Right(res))
          }
      }
      .runSyncUnsafe()
      .queueUrl()

    SqsQueueUrl(queueUrl)
  }

  private def consumeMessagesFrom(queueUrl: SqsQueueUrl) = {
    val req = ReceiveMessageRequest
      .builder
      .queueUrl(queueUrl.str)
      .maxNumberOfMessages(1)
      .visibilityTimeout(3600)
      .build()

    val consumeL = Task
      .async[List[Message]] { cb =>
        sqsClient.receiveMessage(req).whenComplete { (res, ex) =>
          if (ex != null)
            cb(Left(ex))
          else
            cb(Right(res.messages().asScala.toList))
        }
      }

    fs2
      .Stream
      .repeatEval(consumeL)
      .takeWhile(_.nonEmpty)
      .flatMap(fs2.Stream.emits)
      .map(_.body())
      .evalMap(m => Task.fromEither(decode[SqsMessage](m)))
      .compile
      .toList
      .runSyncUnsafe()
  }

}
