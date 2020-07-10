package com.example.io

import java.net.URI

import cats.syntax.traverse._
import cats.instances.list._
import com.example.model._
import com.example.io.SqsStorage
import io.circe.generic.auto._
import io.circe.parser._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.elasticmq.rest.sqs.SQSRestServerBuilder
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.jdk.CollectionConverters._
import scala.util.Random

class SqsStorageSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val sqsInterface = "localhost"
  private val sqsPort = 9325

  private val server = SQSRestServerBuilder
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

  override def afterAll(): Unit = {
    server.stopAndWait()
    sqsClient.close()
  }

  "SqsStorage" should "store values in SQS" in {

    val topic = Topic(Random.alphanumeric.take(6).mkString)

    val queue = createQueue("some-queue")

    val sqsStorage = new SqsStorage(sqsClient, queue, topic)

    val messages = List(
      """{"foo":1,"bar":"asd"}""",
      """{"foo":{"a":1},"bar":[{"asd":true}]}"""
    )

    messages.traverse(sqsStorage.storeBeacon).runSyncUnsafe()

    val expected = messages.map(m => SqsMessage(topic.name, m))

    consumeMessagesFrom(queue) should contain theSameElementsAs expected
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

}
