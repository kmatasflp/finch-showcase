package com.example.monitoring

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import cats.syntax.apply._
import cats.syntax.functor._
import com.codahale.metrics.graphite.GraphiteReporter
import com.codahale.metrics.graphite.PickledGraphite
import com.codahale.metrics.MetricFilter
import com.codahale.metrics.MetricSet
import com.codahale.metrics.jvm._
import com.example.model.{ ContainerId, MetricsPrefix }
import monix.eval.Task
import nl.grons.metrics4.scala.{ DefaultInstrumented, HdrMetricBuilder }

import scala.jdk.CollectionConverters._
import scala.util.Try

trait Metrics extends DefaultInstrumented {

  override protected lazy val metricBuilder =
    new HdrMetricBuilder(metricBaseName, metricRegistry, resetAtSnapshot = true)

  def startGraphiteReporter(
      host: String,
      port: Int,
      prefix: MetricsPrefix,
      containerId: ContainerId
    ): Task[GraphiteReporter] =
    (
      Task.fromTry(registerHeapMetrics()),
      Task.fromTry(registerGarbageCollectorMetrics()),
      Task.fromTry(registerFileDescriptorUsageMetrics())
    ).mapN((_, _, _)).as {
      val pg = new PickledGraphite(new InetSocketAddress(host, port))

      val reporter = GraphiteReporter
        .forRegistry(metricRegistry)
        .prefixedWith(s"$prefix.$containerId")
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL)
        .build(pg)

      reporter.start(1, TimeUnit.MINUTES)

      reporter
    }

  private def registerFileDescriptorUsageMetrics(): Try[Unit] =
    Try(metricRegistry.register("file_descriptor_usage", new FileDescriptorRatioGauge()))

  private def registerHeapMetrics(): Try[Unit] = {

    def registerAll(prefix: String, metricSet: MetricSet): Unit = {
      val metrics = metricSet.getMetrics.asScala

      metrics.foreach {
        case (k, v) =>
          v match {
            case set: MetricSet => registerAll(prefix, set)
            case m              => metricRegistry.register(s"$prefix-$k", m)
          }
      }
    }

    Try(registerAll("memory", new MemoryUsageGaugeSet()))
  }

  private def registerGarbageCollectorMetrics(): Try[Unit] =
    Try(metricRegistry.register("gc", new GarbageCollectorMetricSet()))

}
