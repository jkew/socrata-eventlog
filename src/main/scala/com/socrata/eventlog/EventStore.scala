package com.socrata.eventlog

/**
 * Stores and Retrieves Events
 */
trait EventStore {
  def eventTypes:Seq[String]
  def addEvent(tag:String, values:Map[String, String])
  def getEvents(tag:String, since:Long, filters:Map[String, String]):Seq[Map[String, String]]
  def getEvents(since:Long, filters:Map[String, String]):Seq[Map[String, String]]

}
