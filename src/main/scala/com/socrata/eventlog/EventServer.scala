package com.socrata.eventlog

import com.twitter.finagle.{Service, Http}
import org.jboss.netty.handler.codec.http._
import com.twitter.util.{Await, Future}
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import org.jboss.netty.util.CharsetUtil.UTF_8
import com.twitter.server.TwitterServer
import com.twitter.finagle.http.HttpMuxer


object EventServer extends TwitterServer {

  val jmsUri = flag("jms", "tcp:localhost:-200", "JMX Port to Connect To")

  val helloService: Service[HttpRequest, HttpResponse] = new Service[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest) = {
      val response =
        new DefaultHttpResponse(request.getProtocolVersion, HttpResponseStatus.OK)
      response.setContent(copiedBuffer("hello", UTF_8))
      Future(response)
    }
  }

  def main() = {
    println("Starting server with port: " + adminPort)
    HttpMuxer.addHandler("/hello", helloService)
    Await.ready(adminHttpServer)
  }
}
