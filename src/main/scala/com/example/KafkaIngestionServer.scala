package com.example

import java.net.URI
import java.util.Properties
import java.util.concurrent.TimeUnit

import _root_.io.finch._
import cats.effect.{ ExitCode, Resource }
import cats.syntax.apply._
import cats.syntax.functor._
import com.example.cli.ArgParser
import com.example.io.{ KafkaStorage, SqsStorage, Storage }
import com.example.model.MetricsPrefix
import com.example.monitoring.Metrics
import com.twitter.finagle.Http
import com.twitter.finagle.http.filter.Cors
import com.twitter.finagle.http.{ Request, Response }
import com.twitter.finagle.http.Status.{ ClientError, ServerError, Successful }
import com.twitter.util.{ Future, Stopwatch }
import com.typesafe.scalalogging.StrictLogging
import monix.eval.{ Task, TaskApp }
import org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig }
import org.apache.kafka.common.config.SslConfigs
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import com.twitter.util.Duration

object Filter extends StrictLogging with Metrics {

  def perRouteStats(compiled: Endpoint.Compiled[Task]): Endpoint.Compiled[Task] = {

    def recordRouteFailures(trace: Trace, response: Either[Throwable, Response]) =
      Task.delay {
        response.foreach { res =>
          res.status match {
            case Successful(_)  => metrics.meter(s"$trace.successes").mark()
            case ServerError(_) => metrics.meter(s"$trace.server_failures").mark()
            case _              => ()
          }
        }
      }

    val now = Task.delay(Stopwatch.systemMillis())
    Endpoint.Compiled[Task] { req =>
      for {
        start <- now
        _ <- Task.delay(metrics.meter(s"${req.path}.incoming_requests").mark())
        traceAndResponse <- compiled(req)
        (trace, response) = traceAndResponse
        _ <- recordRouteFailures(trace, response)
        stop <- now
        _ <- Task.delay(metrics.histogram(s"$trace.response_time") += (stop - start))
      } yield {
        traceAndResponse
      }
    }
  }

  def stats(compiled: Endpoint.Compiled[Task]): Endpoint.Compiled[Task] = {

    def recordIncomingRequest(req: Request) =
      Task.delay {
        logger.debug(
          s"""description="Incoming request" request="$req" headers="${req.headerMap}" body="${req.contentString}" """
        )
        metrics.meter(s"incoming_requests.total").mark()
      }

    def recordRejected(response: Either[Throwable, Response]) =
      Task.delay {
        response.foreach { res =>
          res.status match {
            case ClientError(_) => metrics.meter("client_failures").mark()
            case _              => ()
          }
        }
      }

    val now = Task.delay(Stopwatch.systemMillis())
    Endpoint.Compiled[Task] { req =>
      for {
        start <- now
        _ <- recordIncomingRequest(req)
        traceAndResponse <- compiled(req)
        (_, response) = traceAndResponse
        _ <- recordRejected(response)
        stop <- now
        _ <- Task.delay(metrics.histogram(s"response_time") += (stop - start))
      } yield {
        traceAndResponse
      }
    }
  }
}

object KafkaIngestionServer extends TaskApp with Metrics with StrictLogging {

  val port: Int = 8080

  override def run(args: List[String]): Task[ExitCode] =
    ArgParser
      .parse(args)
      .flatMap { params =>
        (kafkaProducer(params.kafkaBootstrapServers), sqsClient(params), graphiteReporter(params))
          .mapN((_, _, _))
          .flatMap {
            case (kafkaProducer, sqsClient, _) =>
              val kafkaStorage = new KafkaStorage(kafkaProducer, params.outputTopic)
              val sqsStorage = new SqsStorage(sqsClient, params.sqsQueueUrl, params.outputTopic)
              server(kafkaStorage, sqsStorage, params.domain)
          }
          .use(_ => Task.never)
          .as(ExitCode.Success)
      }
      .onErrorHandleWith(t =>
        Task(logger.error("""description="Fatal failure" """, t)) *> Task.pure(ExitCode.Error)
      )

  private def kafkaProducer(bootStrapServers: String) = {

    val acquire = Task {
      val kafkaProperties = new Properties()
      kafkaProperties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer"
      )
      kafkaProperties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      kafkaProperties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
      kafkaProperties.put(
        ProducerConfig.MAX_BLOCK_MS_CONFIG,
        sys.props.getOrElse("KAFKA_PRODUCER_MAX_BLOCK_MS", "60000")
      )
      kafkaProperties.put(ProducerConfig.RETRIES_CONFIG, "5")

      kafkaProperties.put(BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)

      new KafkaProducer[Array[Byte], String](kafkaProperties)
    }

    Resource.fromAutoCloseable(acquire)
  }

  private def server(
      kafkaStorage: Storage,
      sqsStorage: Storage,
      domain: String
    ) = {
    val acquire = Task.delay {
      val filters = Filter.perRouteStats _ andThen Filter.stats
      val api = LiveApi(kafkaStorage, sqsStorage, domain)

      val service = Endpoint.toService(
        filters(
          Bootstrap
            .serve[Text.Plain](api.healthCheck :+: api.ingest)
            .compile
        )
      )
      val policy: Cors.Policy = Cors.Policy(
        allowsOrigin = origin => Some(origin),
        allowsMethods = _ => Some(Seq("GET", "POST")),
        allowsHeaders = _ => Some(Seq("Content-Type", "Referer", "User-Agent", "X-Requested-With")),
        maxAge = Some(Duration(1, TimeUnit.DAYS)),
        supportsCredentials = true
      )
      val corsService = new Cors.HttpFilter(policy).andThen(service)

      Http
        .server
        .serve(
          s":$port",
          corsService
        )
    }

    Resource.make(acquire)(s => Task.defer(implicitly[ToAsync[Future, Task]].apply(s.close())))
  }

  private def graphiteReporter(params: ArgParser.CliArgs) =
    Resource.make(
      startGraphiteReporter(
        params.metricsEndpointHost,
        params.metricsEndpointPort,
        MetricsPrefix("kafka-ingestor"),
        params.containerId
      )
    )(r => Task(r.close()))

  private def sqsClient(params: ArgParser.CliArgs) = {

    def newSqsAsyncClientWithRegionAndEndpoint(
        awsRegion: String,
        sqsEndpoint: URI
      ): SqsAsyncClient =
      sqsAsyncClientBuilder
        .region(Region.of(awsRegion))
        .endpointOverride(sqsEndpoint)
        .build

    def newSqsAsyncClientWithRegion(awsRegion: String): SqsAsyncClient =
      sqsAsyncClientBuilder.region(Region.of(awsRegion)).build

    def newSqsAsyncClient: SqsAsyncClient =
      sqsAsyncClientBuilder.build

    def sqsAsyncClientBuilder =
      SqsAsyncClient.builder.httpClientBuilder(NettyNioAsyncHttpClient.builder())

    val acquire = Task.delay(
      params match {
        case ArgParser.CliArgs(_, _, _, _, _, Some(awsRegion), Some(sqsEndpoint), _, _) =>
          newSqsAsyncClientWithRegionAndEndpoint(awsRegion, sqsEndpoint)
        case ArgParser.CliArgs(_, _, _, _, _, Some(awsRegion), None, _, _) =>
          newSqsAsyncClientWithRegion(awsRegion)
        case _ => newSqsAsyncClient
      }
    )

    Resource.fromAutoCloseable(acquire)
  }
}
