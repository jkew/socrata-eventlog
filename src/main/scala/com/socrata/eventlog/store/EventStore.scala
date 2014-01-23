package com.socrata.eventlog.store

import com.twitter.util.Future

/**
 * Stores and Retrieves Events
 */
trait EventStore {
  def eventTypes:Future[Seq[String]]
  def addEvent(tag:String, values:Map[String, String])
  def getEvents(tag:String, since:Long, filters:Map[String, String], skip:Int, limit:Int):Future[Stream[Map[String, String]]]
  def getEvents(since:Long, filters:Map[String, String], skip:Int, limit:Int):Future[Stream[Map[String, String]]]
}

object EventProperties {
  val EVENT_TYPE = "event_type"
  val TIMESTAMP = "ts"
}
