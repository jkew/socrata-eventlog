package com.socrata.eventlog

import scala.collection.mutable
import com.twitter.logging.Logger
import com.twitter.finagle.tracing.Trace

/**
 * EventStore in Memory.
 */
class MemoryEventStore extends InMemoryFilteringEventStore {
  val log:Logger = Logger.get(this.getClass)

  private val events = mutable.Queue[Map[String, String]]()
  private val eventTypeList = mutable.LinkedList[String]()

  protected def getRawEventStream(eventType:Option[String], since:Option[Long], filters:Option[Map[String, String]], skipHint:Int, limitHint:Int) = synchronized {
    Trace.traceService("getRawEventStream", eventType.getOrElse("all")) {
      events.toStream
    }
  }

  def addEvent(eventType: String, values: Map[String, String]) = synchronized {
    Trace.traceService("addEvent", eventType) {
      log.info("Adding event " + eventType + " with values " + values)
      eventTypeList + eventType
      events += (values + (EventProperties.EVENT_TYPE -> eventType) + (EventProperties.TIMESTAMP -> System.currentTimeMillis().toString))
    }
  }

  def eventTypes: Seq[String] = eventTypeList.toSeq
}
