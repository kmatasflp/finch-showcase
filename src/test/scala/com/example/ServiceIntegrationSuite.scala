package com.example

import cats.effect.ExitCode
import com.twitter.finagle.http.{ Request, Response }
import com.twitter.finagle.{ Http, Service }
import com.twitter.util.{ Await, Duration }
import monix.eval.Task
import monix.execution.CancelableFuture
import org.scalatest.Outcome
import monix.execution.Scheduler.Implicits.global
import org.scalatest.FixtureTestSuite

trait ServiceIntegrationSuite {
  self: FixtureTestSuite =>

  def createService(): Task[ExitCode]

  /**
    * Override in implementing classes if a different port is desired for
    * integration tests.
    */
  def port: Int = 8080

  case class FixtureParam(service: Service[Request, Response]) {

    /**
      * Apply the service and await the response.
      */
    def apply(req: Request, timeout: Duration = Duration.fromSeconds(10)): Response =
      Await.result(service(req), timeout)
  }

  /**
    * Provide a fixture containing a client that calls our locally-served
    * service.
    */
  override def withFixture(test: OneArgTest): Outcome = {
    val service: CancelableFuture[ExitCode] = createService().runToFuture
    val client: Service[Request, Response] = Http.newService(s"127.0.0.1:$port")

    try {
      self.withFixture(test.toNoArgTest(FixtureParam(client)))
    }
    finally {
      service.cancel()
      Await.ready(
        client.close()
      )
    }
  }
}
