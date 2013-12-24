package com.socrata.eventlog

import com.twitter.finagle.{Service}
import org.jboss.netty.handler.codec.http._
import com.twitter.util.{Await, Future}
import java.net.{SocketAddress, InetSocketAddress}
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.http.Http


object Main extends TwitterServer {
  val service: Service[HttpRequest, HttpResponse] = new Service[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest) = Future(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK))
  }
  def main(args: Array[String]) {
    println("Hello World!")
    val server = Http.serve(":8888", service)
    onExit {
      server.close()
    }
    Await.ready(server)
  }
}
