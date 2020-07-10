package com.example.fixtures

import net.manub.embeddedkafka.EmbeddedKafka
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

trait RunningKafka extends BeforeAndAfterAll { this: Suite =>

  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedKafka.stop()
  }

}
