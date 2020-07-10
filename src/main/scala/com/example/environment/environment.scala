package com.example

import cats.data.ValidatedNec
import cats.syntax.apply._
import cats.syntax.validated._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs

package object environment {

  private val microserviceToKafkaSslConfigMapping = Map(
    "KAFKA_CLIENT_PRIVATE_KEY_PASSWORD" -> SslConfigs.SSL_KEY_PASSWORD_CONFIG,
    "TRUSTSTORE_JKS_URI" -> SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
    "TRUSTSTORE_PASSWORD" -> SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
    "KEYSTORE_JKS_URI" -> SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
    "KEYSTORE_PASSWORD" -> SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG
  )

  private def getSslProperties: ValidatedNec[String, Map[String, String]] = {
    val setConf = microserviceToKafkaSslConfigMapping.foldLeft(Map.empty[String, String]) {
      case (acc, (msConfKey, kafkaConfKey)) =>
        sys.env.get(msConfKey).fold(acc)(v => acc + (kafkaConfKey -> v))
    }

    if (setConf.isEmpty) {
      Map.empty[String, String].validNec
    }
    else {

      val missingConf = microserviceToKafkaSslConfigMapping.values.toSet.diff(setConf.keySet)

      if (missingConf.isEmpty) {
        (setConf + (CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> "SSL")).validNec
      }
      else {
        s"Following Environment Variables $missingConf are not defined in runtime environment".invalidNec
      }

    }

  }

  private def getBrokerProperties: ValidatedNec[String, Map[String, String]] =
    sys
      .env
      .get("KAFKA_SSL_BROKERS")
      .fold(
        "KAFKA_SSL_BROKERS Environment variable not set".invalidNec[Map[String, String]]
      )(v => Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> v).validNec[String])

  def getClientProperties: ValidatedNec[String, Map[String, String]] =
    (getSslProperties, getBrokerProperties).mapN(_ ++ _)
}
