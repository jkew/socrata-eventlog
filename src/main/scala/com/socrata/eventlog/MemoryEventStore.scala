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
    val newValue = events.getOrElse(eventType, Vector.empty) :+ values
    events += eventType -> newValue
  }

  def getEvents(eventType: String, since: Long = 0, filters: Map[String, String]):Seq[Map[String, String]] = {
    log.info("Looking up " + eventType + " since " + since + " with filters " + filters)
    val e = synchronized { events.getOrElse(eventType, Vector.empty) }
    if(filters.isEmpty) e
    else e.filter { m => filters.forall { case (k, v) => v.equals(m.get(k)) } }
  }

  def eventTypes: Seq[String] = events.keySet.toSeq
}
