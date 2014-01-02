package com.socrata.eventlog

import com.twitter.util.{Duration, Await, Future}
import com.twitter.server.TwitterServer
import com.twitter.finagle.http._
import com.netflix.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.netflix.curator.framework.state.{ConnectionStateListener, ConnectionState}
import com.netflix.curator.framework.api.UnhandledErrorListener
import com.netflix.curator.{RetrySleeper, RetryPolicy}
import com.twitter.finagle.http.path._
import com.twitter.finagle
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import com.twitter.finagle.zipkin.thrift.ZipkinTracer
import scala.Some
import com.twitter.finagle.builder.ServerBuilder
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import com.twitter.finagle.tracing.Trace


class ErrorService(method:Object, path:Path) extends finagle.Service[Request, Response] {
  def apply(request: Request) = {
    val response = Response(request.getProtocolVersion, HttpResponseStatus.NOT_FOUND)
    Future(response)
  }
}

object Router {
  val service = RequestRouter.byRequest {
    request =>
    (request.method, Path(request.path)) match {
      case Method.Get -> Root / "eventlog" / eventType / Long(since) => new EventLogService(Some(eventType), since, EventServer.store)
      case Method.Get -> Root / "eventlog" / Long(since) => new EventLogService(None, since, EventServer.store)
      case Method.Get -> Root / "eventlog" => new EventListService(EventServer.store)
      case m -> p => new ErrorService(m, p)
    }
  }
}

/**
 * Tests:
 *   - Verify start/stop is clean
 *   - Verify start/stop with initially down ZK is clean
 *   - Verify start/stop with failing ZK is clean
 */
object EventServer extends TwitterServer with UnhandledErrorListener with ConnectionStateListener {

  val zkConnect        = flag("zk", "localhost:2181", "zookeeper connection string")
  val amqUri           = flag("amqUri", "tcp://localhost:61616", "amq connection string")
  val zipkinHost       = flag("zipkinHost", "localhost", "hostname for zipkin/scribe")
  val zipkinPort       = flag("zipkinPort", 9410, "port for zipkin collector/scribe")
  val zipkinSampleRate = flag("zipkinSampleRate", 1.0F, "zipkin sample rate")

  val store = new MemoryEventStore

  val zkMinRetryInterval = 60000
  val zkRetries = 1

  // Very pessimistic; if we cannot connect to zookeeper; don't bother retrying
  val client = CuratorFrameworkFactory.newClient(zkConnect(), new RetryPolicy {
    def allowRetry(retryCount: Int, elapsedTimeMs: Long, sleeper: RetrySleeper) = {
      Thread.sleep(60000)
      false
    }
  })
  client.getUnhandledErrorListenable.addListener(this)
  client.getConnectionStateListenable.addListener(this)
  val processor = new EventLogProcessor(client, store, amqUri())

  def main() = {
    log.info("Starting server with port: " + adminPort)

    // Setup zipkin
    //val zipkin =  ZipkinTracer.mk(zipkinHost(), zipkinPort(), statsReceiver, zipkinSampleRate())

    //val address =new InetSocketAddress(7777)
    //ServerBuilder()
    //    .codec(new RichHttp[Request](finagle.http.Http()))      // Use HTTP
    //    .bindTo(address)                                        // Bind to Port
    //    .name("eventlog")                                       // Our Name
    //    .requestTimeout(Duration(500, TimeUnit.MILLISECONDS))   // All Requests should be served in under 500ms
    //    .tracer(zipkin)                                         // Add in Tracing
    //    .build(Router.service)                                  // Add the service

    HttpMuxer.addRichHandler("/eventlog/", Router.service)
    // Tracing is enabled by default in ServerBuilder
    Trace.disable()

    // Connect to Zookeeper
    client.start()

    // Setup exit handler
    onExit {
      log.info("Shutting down processor")
      processor.close()
      log.info("Shutting down ZK Client")
      client.close()
      log.info("Shutting down http server")
      adminHttpServer.close()
    }

    // Event Message Processor
    new Thread(processor).start()

    // Ready to Serve Requests
    Await.ready(adminHttpServer)

  }

  // Any negative state change to ZK will result in a complete shutdown of this server
  def stateChanged(client: CuratorFramework, newState: ConnectionState) {
    if (ConnectionState.LOST == newState || ConnectionState.SUSPENDED == newState) {
      log.error("Lost or Suspended ZK Connection. Abandoning all hope.")
      die()
    }
  }

  def unhandledError(message: String, e: Throwable) {
    log.error("Unhandled ZK Error. Abandoning all hope. " + message, e)
    die()
  }
  
  def die() {
    System.exit(1)
  }
}
