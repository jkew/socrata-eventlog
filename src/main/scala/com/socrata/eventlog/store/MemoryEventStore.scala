package com.socrata.eventlog.store

import scala.collection.mutable
import com.twitter.logging.Logger
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Future, RingBuffer}

/**
 * EventStore in Memory.
 */
class MemoryEventStore(size:Int) extends InMemoryFilteringEventStore {
  val log:Logger = Logger.get(this.getClass)

  private val events = new RingBuffer[Map[String, String]](size)
  private val eventTypeList = mutable.LinkedList[String]()

  protected def getRawEventStream(eventType:Option[String], since:Option[Long], filters:Option[Map[String, String]], skipHint:Int, limitHint:Int) = synchronized {
    Trace.traceService("getRawEventStream", eventType.getOrElse("all")) {
      Future(events.toStream)
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
