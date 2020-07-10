package com.example.cli

import java.net.URI

import com.example.model.{ ContainerId, SqsQueueUrl, Topic }
import monix.eval.Task

object ArgParser {

  case class CliArgs(
      outputTopic: Topic,
      sqsQueueUrl: SqsQueueUrl,
      metricsEndpointHost: String,
      metricsEndpointPort: Int,
      containerId: ContainerId,
      aswRegion: Option[String],
      awsSqsEndpoint: Option[URI],
      kafkaBootstrapServers: String,
      domain: String
    )

  private val zero =
    CliArgs(Topic(null), SqsQueueUrl(null), null, 0, ContainerId(null), None, None, null, null)
  private val parser = new scopt.OptionParser[CliArgs]("scopt") {
    head("Kafka Ingestor")
    help("help").text("Write content that hits HTTP endpoint to Kafka")

    opt[String]("outputTopic")
      .text("Output topic")
      .required()
      .action((x, c) => c.copy(outputTopic = Topic(x)))

    opt[String]("sqsQueueUrl")
      .text("Sqs queue url")
      .required()
      .action((x, c) => c.copy(sqsQueueUrl = SqsQueueUrl(x)))

    opt[String]("metricsEndpointHost")
      .text("Graphite agent host")
      .required()
      .action((x, c) => c.copy(metricsEndpointHost = x))

    opt[Int]("metricsEndpointPort")
      .text("Graphite agent port")
      .required()
      .action((x, c) => c.copy(metricsEndpointPort = x))

    opt[String]("containerId")
      .text("Container id")
      .required()
      .action((x, c) => c.copy(containerId = ContainerId(x)))

    opt[String]("awsRegion")
      .text("Aws region")
      .optional()
      .action((x, c) => c.copy(aswRegion = Some(x)))

    opt[String]("awsSqsEndpoint")
      .text("Aws Sqs Endpoint")
      .optional()
      .action((x, c) => c.copy(awsSqsEndpoint = Some(URI.create(x))))

    opt[String]("kafkaBootstrapServers")
      .text("Kafka bootstrap servers")
      .required()
      .action((x, c) => c.copy(kafkaBootstrapServers = x))

    opt[String]("domain")
      .text("Domain")
      .required()
      .action((x, c) => c.copy(domain = x))
  }

  def parse(args: List[String]): Task[CliArgs] =
    Task.defer(
      Task.fromEither(
        parser
          .parse(args, zero)
          .toRight(new IllegalArgumentException("Arg parser unable to initialize! Fatal Error"))
      )
    )
}
