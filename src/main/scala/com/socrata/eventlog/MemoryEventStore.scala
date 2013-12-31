package com.socrata.eventlog

import scala.collection.mutable
import com.twitter.logging.Logger

/**
 * EventStore in Memory.
 */
class MemoryEventStore extends EventStore {
  val log:Logger = Logger.get(this.getClass)

  private val events = new mutable.HashMap[String, Vector[Map[String, String]]]

  def addEvent(eventType: String, values: Map[String, String]) = synchronized {
    log.info("Adding event " + eventType + " with values " + values)
    val newValue = events.getOrElse(eventType, Vector.empty) :+  (values + ("event_type" -> eventType) + ("ts" -> System.currentTimeMillis().toString))
    events += eventType -> newValue
  }

  def getEvents(eventType: String, since:Long, filters: Map[String, String]):Stream[Map[String, String]] = {
    log.info("Looking up " + eventType + " since " + since + " with filters " + filters)
    val e = synchronized { events.getOrElse(eventType, Vector.empty).toStream }
    propertyFilter(timeFilter(e, since), filters)
  }

  def getEvents(since:Long, filters: Map[String, String]):Stream[Map[String,String]] = {
    val e = eventTypes flatMap {
       eventType => getEvents(eventType, since, filters)
    }
    sortByTimeFilter(e.toStream)
  }

  def timeFilter(e:Stream[Map[String, String]], since:Long = 0):Stream[Map[String, String]] = {
    if (since < 0) e
    else e.filter { m => since <= m.get("ts").get.toLong}
  }

  def propertyFilter(e:Stream[Map[String, String]], filters: Map[String, String] = Map()):Stream[Map[String, String]] = {
    if(filters.isEmpty) e
    else e.filter { m => filters.forall { case (k, v) => v.equals(m.get(k).get) } }
  }

  def sortByTimeFilter(e:Stream[Map[String,String]]) = {
    e.sortBy ( m => m.get("ts").get.toLong)
  }

  def eventTypes: Seq[String] = events.keySet.toSeq
}
