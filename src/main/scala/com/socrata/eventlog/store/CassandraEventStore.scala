package com.socrata.eventlog.store

import com.netflix.astyanax._
import com.netflix.astyanax.AstyanaxContext.Builder
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.{OperationResult, NodeDiscoveryType}
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import java.util.{UUID, Properties}
import com.netflix.astyanax.model.{ByteBufferRange, ColumnList, ColumnFamily, ConsistencyLevel}
import com.netflix.astyanax.serializers.StringSerializer
import scala.Predef._
import com.netflix.astyanax.util.RangeBuilder
import com.twitter.util.{Promise, Future}
import com.google.common.util.concurrent.ListenableFuture
import java.util.concurrent.Executors
import scala.collection.JavaConverters._

/**
 * Cassandra Event Storage
 *
 *  {eventType}-domain-[domainId] | (ts|uuid) ...
 *  {eventType}-id-[domainId] | (ts|uuid) ...
 *  {eventType} | (ts|uuid) ...
 *  {eventType}-[uuid] | event_type | ts | ... values ...
 *
 **/

class CassandraEventStore(daysTTL:Int) extends InMemoryFilteringEventStore {
  import CassandraEventStore._

  protected def getRawEventStream(eventType: Option[String], since: Option[Long], filters: Option[Map[String, String]], skipHint: Int, limitHint: Int) = {
    val event = eventType match {
      case Some(e) => e
      case None => throw new UnsupportedOperationException("Event Type required")
    }
    val range = new RangeBuilder().setStart(since.getOrElse(0L).toString).build

    // For each filter perform a column range query on timestamp to collect uuids
    val uuidMatches:Iterable[Future[Iterable[String]]] = filters match {
      case Some(f:Map[String, String]) => {
        f map {
          case ((k:String, v:String))  => {
              queryIndex(getIndexKey(event, k, v), range)
          }
        }
      }
    }
    //   collect intersection of uuids of each filter query for whitelist
    val whitelist:Future[Set[String]] = if (filters.isDefined && filters.size > 0) {
      //   flatten out the filter whitelist; if it exists
      Future.collect(uuidMatches.toSeq).flatten
    } else {
      //   perform range scan on {eventType} for timestamp to create whitelist
      queryIndex(getIndexKey(event), range)
    }

    for {
      uuids <- whitelist
      events <- uuids map { uuid => queryEvent(event, uuid) }
    } yield List(events).toStream
  }

  def eventTypes = null

  def queryEvent(eventType:String, uuid:String):Future[Map[String, String]] = {
     val f:ListenableFuture[OperationResult[ColumnList[String]]] = context.getClient.prepareQuery(eventCF).getKey(getEventKey(eventType, uuid)).executeAsync()
     mapJUFutureToTwitterFuture(f) map {
        opResults => opResults.getResult.asScala.toMap[String, String]
      }
  }

  /**
   * Query the cassandra property index and return a twitter future over an iterable list of uuids
   * Should be fully asynchronous
   * @param key   index row key
   * @param range filter range
   * @return      twitter future
   */
  def queryIndex(key:String, range:ByteBufferRange):Future[Set[String]] = {
    val f:ListenableFuture[OperationResult[ColumnList[String]]] = context.getClient.prepareQuery(indexCF).getKey(key).withColumnRange(range).executeAsync()

    mapJUFutureToTwitterFuture(f) map {
      opResults => opResults.getResult.asScala.toSet map {
        col => col.getStringValue
      }
    }

  }

  def mapJUFutureToTwitterFuture[A](f:ListenableFuture[A]):Future[A] = {
    val p = new Promise[A]
    // Convert the java Future to a Promise
    f.addListener(new Runnable() {
      def run() {
        p.setValue(f.get())
      }
    }, resultCallbackThreadpool)

    p map {
      result => result
    }
  }

  def addEvent(tag: String, values: Map[String, String]) = {
    val uuid = UUID.randomUUID().toString

    val mutation = context.getClient.prepareMutationBatch

    // Write event data
    val eventMutation = getColumnMutation(mutation, eventCF, getEventKey(tag, uuid))
    putValues(eventMutation, values)

    // Write ts-range index to the {eventType} column family
    val ts = System.currentTimeMillis().toString
    val tsMutation = getColumnMutation(mutation, indexCF, getIndexKey(tag, "all", "all"))
    tsMutation.putColumn(ts, uuid, EVENT_TTL)

    // Write property index lookup
    values foreach {
      case (k: String, v: String) => {
        val propertyMutation = getColumnMutation(mutation, indexCF, getIndexKey(tag, k, v))
        propertyMutation.putColumn(ts, uuid, EVENT_TTL)
      }
    }

    mutation.executeAsync()
  }
}


object CassandraEventStore {
  lazy val context = initializeContext(System.getProperties)
  val EVENT_TTL = 7 * 24 * 60 * 60
  val indexCF = getIndexColumnFamily()
  val eventCF = getEventColumnFamily()
  val resultCallbackThreadpool = Executors.newFixedThreadPool(16)


  def getColumnMutation(mutation:MutationBatch, cf:ColumnFamily[String, String], key:String) = {
    mutation
      .setConsistencyLevel(ConsistencyLevel.CL_ONE)
      .withRow(cf, key)
      .setDefaultTtl(CassandraEventStore.EVENT_TTL)
  }

  def putValues(colm:ColumnListMutation[String], values: Map[String, String]) = {
    var m = colm
    values foreach {
      case (k: String, v: String) => m = m.putColumn(k, v)
    }
  }

  def getEventKey(eventType:String, uuid:String) = {
    eventType + "-" + uuid
  }

  def getIndexKey(eventType:String) = {
    eventType + "all"
  }

  def getIndexKey(eventType:String, key:String, value:String) = {
    eventType + "-" + key.hashCode + "-" + value.hashCode
  }

  def getEventColumnFamily():ColumnFamily[String, String] = {
    new ColumnFamily[String, String]("event-log", StringSerializer.get(), StringSerializer.get())
  }

  def getIndexColumnFamily():ColumnFamily[String, String] = {
    new ColumnFamily[String, String]("event-log-index", StringSerializer.get(), StringSerializer.get())
  }

  def initializeContext(conf:Properties):AstyanaxContext[Keyspace] = {
    val seeds = conf.getProperty("cassandra.servers")
    val keyspace = conf.getProperty("cassandra.keyspace")
    val sotimeout = conf.getProperty("cassandra.sotimeout").toInt
    val connections = conf.getProperty("cassandra.maxpoolsize").toInt
    val cxt:AstyanaxContext[Keyspace] = new Builder()
      .forKeyspace(keyspace)
      .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
      .setDiscoveryType (NodeDiscoveryType.RING_DESCRIBE)
    )
      .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("EventLogPool")
      .setConnectTimeout(sotimeout)
      .setTimeoutWindow(sotimeout)
      .setMaxConnsPerHost(connections)
      .setSeeds(seeds))
      .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
      .buildKeyspace(ThriftFamilyFactory.getInstance())
    cxt.start()
    cxt
  }
}
