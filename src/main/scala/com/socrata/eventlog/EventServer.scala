package com.socrata.eventlog

import com.twitter.util.Await
import com.twitter.server.TwitterServer
import com.twitter.finagle.http.{Request, Response, Method, HttpMuxer}
import com.netflix.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.netflix.curator.framework.state.{ConnectionStateListener, ConnectionState}
import com.netflix.curator.framework.api.UnhandledErrorListener
import com.netflix.curator.{RetrySleeper, RetryPolicy}
import com.twitter.finagle.http.service.RoutingService
import com.twitter.finagle.http.path._
import com.twitter.finagle.Service
import com.twitter.finagle
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import com.twitter.util.Future


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

  val zkConnect = flag("zk", "localhost:2181", "zookeeper connection string")
  val amqUri = flag("amqUri", "tcp://localhost:61616", "amq connection string")
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


    //HttpMuxer.addHandler("/eventlog/", new EventLogService(store))
    HttpMuxer.addRichHandler("/eventlog/", Router.service)
    // Connect to Zookeeper
    client.start()
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
