name := "finch-showcase"
organization := "km"

version := "2.0.0"

scalaVersion := "2.13.3"

resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.jcenterRepo,
  Resolver.sonatypeRepo("releases"),
  "Confluent" at "https://packages.confluent.io/maven/"
)

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.7.1",
  "com.github.finagle" %% "finchx-core" % "0.32.1",
  "io.monix" %% "monix" % "3.2.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.apache.kafka" % "kafka-clients" % "2.4.1",
  "software.amazon.awssdk" % "sqs" % "2.11.12",
  "com.github.cb372" %% "cats-retry" % "1.1.0",
  "nl.grons" %% "metrics4-scala" % "4.1.5",
  "nl.grons" %% "metrics4-scala-hdr" % "4.1.5",
  "io.dropwizard.metrics" % "metrics-jvm" % "4.1.5",
  "io.dropwizard.metrics" % "metrics-graphite" % "4.1.5",
  "io.circe" %% "circe-parser" % "0.13.0",
  "io.circe" %% "circe-generic" % "0.13.0",
  "at.favre.lib" % "bytes" % "1.3.0",
  "org.scalatest" %% "scalatest" % "3.1.1" % Test,
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.4.1" % Test,
  "org.elasticmq" %% "elasticmq-rest-sqs" % "0.15.6" % Test,
  "co.fs2" %% "fs2-io" % "2.4.0" % Test
)

dependencyOverrides ++= Seq(
  "io.dropwizard.metrics" % "metrics-core" % "4.1.5"
)

scalacOptions ++= Seq(
  "-explaintypes",
  "-deprecation",
  "-feature",
  "-Xfatal-warnings",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Wunused",
  "-Xlint:inaccessible",
  "-Xlint:infer-any",
  "-Xlint:stars-align",
  "-Xlint:nonlocal-return",
  "-Xlint:constant",
  "-Xlint:adapted-args"
)

excludeDependencies ++= Seq(
  "log4j" % "log4j",
  "org.slf4j" % "slf4j-log4j12"
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF")              => MergeStrategy.discard
  case PathList("org", "apache", xs @ _*)         => MergeStrategy.first
  case "about.html"                               => MergeStrategy.rename
  case "reference.conf"                           => MergeStrategy.concat
  case _                                          => MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheOutput = false)

enablePlugins(DockerPlugin)

dockerfile in docker := {
  // The assembly task generates a fat JAR file
  val artifact: File = assembly.value
  val artifactTargetPath = s"/app/${artifact.name}"

  // format: off
  new Dockerfile {
    from("openjdk:11-jre-slim")
    expose(8080)
    add(artifact, artifactTargetPath)
    entryPoint(
      "java", "-cp", artifactTargetPath,
      "-Xms2G",
      "-Xmx2G",
      "-XX:+PrintCommandLineFlags",
      "-XX:MaxGCPauseMillis=100",
      "-XX:+UnlockExperimentalVMOptions",
      "-XX:+EnableJVMCI",
      "-XX:+UseJVMCICompiler",
      "com.example.KafkaIngestionServer",
      "--outputTopic", "beacons",
      "--sqsQueueUrl", "beacons-to-retry",
      "--metricsEndpointPort", "2003",
      "--metricsEndpointHost", "graphite.example.com",
      "--containerId", "some-container-id",
      "--domain", "localhost",
      "--kafkaBootstrapServers", "kafka.example.com:9092"
    )
  }
  // format: on
}

imageNames in docker := Seq(
  // Sets the latest tag
  ImageName(s"${organization.value}/${name.value}:latest"),
  // Sets a name with a tag that contains the project version
  ImageName(
    namespace = Some(organization.value),
    repository = name.value,
    tag = Some("v" + version.value)
  )
)
