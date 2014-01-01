package com.socrata.eventlog

import com.twitter.finagle.Service
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.util.CharsetUtil._
import com.twitter.util.Future
import scala.collection.JavaConverters._
import com.rojoma.json.util.JsonUtil
import com.twitter.logging.Logger
import com.twitter.finagle.http.{Response, Request}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JNumber, JString}

/**
 * Retrieves Events
 */
class EventLogService(eventType:Option[String], since:Long, store:EventStore) extends Service[Request, Response] {
  import EventLogService._

  def apply(request:Request) = {
    val response = Response(request.getProtocolVersion, HttpResponseStatus.OK)
    val params = request.getParams().asScala map {
      entry => (entry.getKey, entry.getValue)
    }
    val (filters, paging) = params.toMap.partition {
      case (SKIP_PARAM, v) => false
      case (LIMIT_PARAM, v) => false
      case (k, v) => true
    }

    val skip = paging.get(SKIP_PARAM).getOrElse(SKIP_DEFAULT).toInt
    val limit = paging.get(LIMIT_PARAM).getOrElse(LIMIT_DEFAULT).toInt

    Future {
      val allEvents = if (eventType.isDefined) store.getEvents(eventType.get, since,filters, skip, limit)
      else store.getEvents(since, filters, skip, limit)
      response.setContentTypeJson()
      response.setContent(copiedBuffer(JsonUtil.renderJson(allEvents.toList, pretty=true), UTF_8))
      response
    }
  }
}

object EventLogService {
  val log:Logger = Logger.get(this.getClass)
  val SKIP_PARAM = "skip"
  val LIMIT_PARAM = "limit"
  val SKIP_DEFAULT = "0"
  val LIMIT_DEFAULT = "1000"
}

class EventListService(store:EventStore) extends Service[Request, Response] {
  val log:Logger = Logger.get(this.getClass)

  def apply(request:Request) = {
    val response = Response(request.getProtocolVersion, HttpResponseStatus.OK)
    val eventList = store.eventTypes
    response.setContent(copiedBuffer(JsonUtil.renderJson(eventList.toList, pretty=true), UTF_8))
    Future(response)
  }
}
