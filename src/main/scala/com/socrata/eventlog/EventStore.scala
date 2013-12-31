package com.socrata.eventlog

/**
 * Stores and Retrieves Events
 */
trait EventStore {
  def eventTypes:Seq[String]
  def addEvent(tag:String, values:Map[String, String])
  def getEvents(tag:String, since:Long = 0, filters:Map[String, String] = Map()):Seq[Map[String, String]]
}
