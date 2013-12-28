package com.socrata.eventlog

import com.twitter.finagle.{Service, Http}
import org.jboss.netty.handler.codec.http._
import com.twitter.util.{Await, Future}
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import org.jboss.netty.util.CharsetUtil.UTF_8
import com.twitter.server.TwitterServer
import com.twitter.finagle.http.HttpMuxer
import com.netflix.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.netflix.curator.retry.RetryNTimes
import com.netflix.curator.framework.state.{ConnectionStateListener, ConnectionState}
import com.netflix.curator.framework.api.UnhandledErrorListener
import scala.util.Random


object EventServer extends TwitterServer with UnhandledErrorListener with ConnectionStateListener {

  val zkConnect = flag("zk", "localhost:2181", "zookeeper connection string")
  val zkMinRetryInterval = 60000
  val zkRetries = 10
  val client = CuratorFrameworkFactory.newClient(zkConnect(), new RetryNTimes(zkRetries, zkMinRetryInterval + Random.nextInt(zkMinRetryInterval)))
  client.getUnhandledErrorListenable.addListener(this)
  client.getConnectionStateListenable.addListener(this)
  val processor = new EventLogProcessor(client)

  val helloService: Service[HttpRequest, HttpResponse] = new Service[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest) = {
      val response =
        new DefaultHttpResponse(request.getProtocolVersion, HttpResponseStatus.OK)
      response.setContent(copiedBuffer("hello", UTF_8))
      Future(response)
    }
  }

  def main() = {
    log.info("Starting server with port: " + adminPort)
    HttpMuxer.addHandler("/hello", helloService)

    // Connect to Zookeeper
    client.start()

    onExit {
      processor.close()
      client.close()
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
    onExit()
    System.exit(1)
  }
}
