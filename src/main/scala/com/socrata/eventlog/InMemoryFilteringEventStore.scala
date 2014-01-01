package com.socrata.eventlog

/**
 * Applies additional in-memory filtering a top an event stream for backends
 */
abstract class InMemoryFilteringEventStore extends EventStore {

  protected def getRawEventStream(eventType:Option[String], since:Option[Long], filters:Option[Map[String, String]], skipHint:Int, limitHint:Int):Stream[Map[String,String]]

  def getEvents(eventType: String, since:Long, filters: Map[String, String], skip:Int, limit:Int):Stream[Map[String, String]] = {
    pageFilter(skip, limit,
      sortByTimeFilter(
        propertyFilter(filters,
          timeFilter(since,
            eventFilter(eventType,
              getRawEventStream(Option(eventType), Option(since), Option(filters), skip, limit))))))
  }

  def getEvents(since:Long, filters: Map[String, String], skip:Int, limit:Int):Stream[Map[String,String]] = {
    pageFilter(skip, limit,
      sortByTimeFilter(
        propertyFilter(filters,
          timeFilter(since,
            getRawEventStream(None, Option(since), Option(filters), skip, limit)))))
  }

  def timeFilter(since:Long, e:Stream[Map[String, String]]):Stream[Map[String, String]] = {
    if (since < 0) e
    else e.filter { m => since <= m.get(EventProperties.TIMESTAMP).get.toLong}
  }

  def eventFilter(eventType: String, e:Stream[Map[String,String]]) = {
    e.filter { m => m.get(EventProperties.EVENT_TYPE) == eventType }
  }

  def propertyFilter(filters: Map[String, String], e:Stream[Map[String, String]]):Stream[Map[String, String]] = {
    if(filters.isEmpty) e
    else e.filter { m => filters.forall { case (k, v) => v.equals(m.get(k).get) } }
  }

  def sortByTimeFilter(e:Stream[Map[String,String]]) = {
    e.sortBy ( m => m.get(EventProperties.TIMESTAMP).get.toLong)
  }

  def pageFilter(skip:Int, limit:Int, e:Stream[Map[String,String]]) = {
    e.drop(skip).take(limit)
  }

}
