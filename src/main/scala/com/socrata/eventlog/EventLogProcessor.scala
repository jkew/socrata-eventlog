package com.socrata.eventlog

import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.recipes.leader.LeaderLatch
import com.twitter.logging.Logger
import java.util.concurrent.Executors
import com.socrata.eurybates._
import com.socrata.eurybates.Message
import org.apache.zookeeper.CreateMode
import java.util.UUID
import com.rojoma.json.jpath.JPath
import com.rojoma.json.ast.{JNumber, JString, JValue, JObject}
import com.rojoma.json.zipper.JsonZipper

/**
 * Takes events off a eurybates queue and puts them into a backend
 */
class EventLogProcessor(client:CuratorFramework, store:EventStore, amqUrl:String) extends Runnable {
  val log:Logger = Logger.get(this.getClass)
  val clientId = UUID.randomUUID().toString
  val registration = new EurybatesEventRegister(client)

  def run() {
    log.info("Processor Starting")
    val listenUrls =  amqUrl.split(' ').filter(_.nonEmpty)
    val connFactories = listenUrls.map(new org.apache.activemq.ActiveMQConnectionFactory(_))
    val connections = connFactories.map(_.createConnection())
    connections.foreach(_.start())
    val executor = Executors.newCachedThreadPool()
    val consumers = connections.map(new com.socrata.eurybates.activemq.ActiveMQServiceConsumer(_, clientId, executor, logger, Map[ServiceName, Service]("eventlog" ->  new GenericConsumer(store))))
    consumers.foreach(_.start())
    log.info("Processor Started")
    registration.stayRegistered
    log.info("Processor Stopping")
    consumers.foreach(_.stop())
    log.info("Processor Stopped")

  }

  def logger(serviceName: ServiceName, msg: String, e: Throwable) {
    log.error("Unexpected error in " + serviceName + " handling message: " + msg, e)
  }

  def close() {
    log.info("Closing Processor")
    registration.close
  }

}

class GenericConsumer(store:EventStore) extends Service {
  def messageReceived(message: Message) = {
    val tag = message.tag.toString
    val eventData = message.details.asInstanceOf[JObject].fields.flatMap {
      case (key:String,jv:JObject) => jv.get("id") match {
        case Some(id) => List((key, id.asInstanceOf[JString].string))
        case None => Nil
      }
      case (key:String,jv:JNumber) => List((key, jv.toString))
      case (key:String,jv:JString) => List((key, jv.string))

    }
    store.addEvent(tag.toLowerCase, eventData.toMap[String, String])
    println(tag + ": " + eventData)
  }
}
