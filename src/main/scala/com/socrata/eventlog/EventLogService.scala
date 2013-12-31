package com.socrata.eventlog

import com.twitter.finagle.Service
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.util.CharsetUtil._
import com.twitter.util.Future
import scala.collection.JavaConverters._
import com.google.common.base.Splitter
import com.rojoma.json.util.JsonUtil
import com.twitter.logging.Logger

/**
 * Exposes Events
 */
class EventLogService(store:EventStore) extends Service[HttpRequest, HttpResponse] {
  val splitter = Splitter.on('/').omitEmptyStrings()
  val log:Logger = Logger.get(this.getClass)

  def apply(request:HttpRequest) = {
    val response =
      new DefaultHttpResponse(request.getProtocolVersion, HttpResponseStatus.OK)
    val uri = request.getUri()
    val params = new QueryStringDecoder(uri).getParameters().asScala map {
      case (key:String, value:List[String]) => (key, value.get(0))
    }

    val paths = splitter.split(uri).iterator()
    val allEvents = paths.asScala map {
        case "eventlog" => Seq()
        case tag:String => store.getEvents(tag, 0, params.toMap)
        case _ => Nil
      }

    response.setContent(copiedBuffer(JsonUtil.renderJson(allEvents.toList, pretty=true), UTF_8))
    Future(response)
  }
}
