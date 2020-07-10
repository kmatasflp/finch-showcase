package com.example.io

import cats.instances.list._
import cats.syntax.functor._
import cats.syntax.traverse._
import com.example.model.Topic
import com.example.io.KafkaStorage
import monix.execution.Scheduler.Implicits.global
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.Codecs.stringKeyValueCrDecoder
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.compatible.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class KafkaStorageSpec
    extends AnyFlatSpecLike
    with EmbeddedKafka
    with BeforeAndAfterAll
    with Eventually
    with Matchers {

  implicit private val keySerializer: Serializer[Array[Byte]] = new ByteArraySerializer()
  implicit private val valueSerializer: Serializer[String] = new StringSerializer()
  implicit private val valueDeserializer: Deserializer[String] = new StringDeserializer()

  "KafkaStorage" should "store values in Kafka" in {

    withRunningKafka {

      val topic = Topic(Random.alphanumeric.take(6).mkString)
      val messages = List("foo", "bar")

      EmbeddedKafka.createCustomTopic(topic.name)

      EmbeddedKafka
        .withProducer[Array[Byte], String, Unit] { producer =>
          val storage = new KafkaStorage(producer, topic)

          messages.traverse(storage.storeBeacon).as(()).runSyncUnsafe()
        }

      EmbeddedKafka.withConsumer[String, String, Assertion] { consumer =>
        eventually {
          val values = consumer.consumeLazily[(String, String)](topic.name).take(2).toList.map(_._2)
          values should contain theSameElementsAs messages
        }
      }
    }
  }
}
