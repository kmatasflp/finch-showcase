package com.example.io

import _root_.io.circe.generic.auto._
import _root_.io.circe.syntax._
import com.example.model.{ SqsMessage, SqsQueueUrl, Topic }
import monix.eval.Task
import org.apache.kafka.clients.producer.{ Producer, ProducerRecord, RecordMetadata }
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

final case class StorageFailure(cause: Exception) extends Exception(cause)

trait Storage {
  def storeBeacon(what: String): Task[Unit]
}

class KafkaStorage(
    private val kafkaProducer: Producer[Array[Byte], String],
    private val topic: Topic
  ) extends Storage {

  private def store(record: ProducerRecord[Array[Byte], String]): Task[Unit] =
    Task
      .async[Unit] { cb =>
        kafkaProducer.send(
          record,
          (_: RecordMetadata, exception: Exception) =>
            if (exception != null) {
              cb(Left(exception))
            }
            else {
              cb(Right(()))
            }
        )
      }
      .onErrorRecoverWith {
        case e: Exception => Task.raiseError(StorageFailure(e))
      }

  override def storeBeacon(what: String): Task[Unit] =
    store(new ProducerRecord[Array[Byte], String](topic.name, null, what))
}

class SqsStorage(
    private val client: SqsAsyncClient,
    private val queueUrl: SqsQueueUrl,
    private val topic: Topic
  ) extends Storage {

  private def pushToQueue(body: SqsMessage) = {

    val req =
      SendMessageRequest.builder.queueUrl(queueUrl.str).messageBody(body.asJson.noSpaces).build

    Task
      .async[Unit] { cb =>
        client.sendMessage(req).whenComplete { (_, ex) =>
          if (ex != null)
            cb(Left(ex))
          else
            cb(Right(()))
        }
      }
      .onErrorRecoverWith {
        case e: Exception => Task.raiseError(StorageFailure(e))
      }
  }

  def storeBeacon(what: String): Task[Unit] =
    pushToQueue(SqsMessage(topic.name, what))
}
