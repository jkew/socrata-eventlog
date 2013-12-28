package com.socrata.eventlog

import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.recipes.leader.LeaderLatch
import com.twitter.logging.Logger
import java.util.concurrent.Executors
import com.socrata.eurybates._
import com.socrata.eurybates.Message

/**
 * Takes events off a eurybates queue and puts them into a backend
 */
class EventLogProcessor(client:CuratorFramework, amqUrl:String) extends Runnable {
  val log:Logger = Logger.get(this.getClass)
  val zkPath = "/eventlog/leader"
  val leader = new LeaderLatch(client, zkPath)

  def run() {
    leader.start()
    leader.await() // block forever until we are a leader
    log.info("I am the leader!")

    val listenUrls =  amqUrl.split(' ').filter(_.nonEmpty)
    val connFactories = listenUrls.map(new org.apache.activemq.ActiveMQConnectionFactory(_))
    val connections = connFactories.map(_.createConnection())
    connections.foreach(_.start())
    val executor = Executors.newCachedThreadPool()
    val consumers = connections.map(new com.socrata.eurybates.activemq.ActiveMQServiceConsumer(_, leader.getId, executor, logger, Map[ServiceName, Service]("eventlog" ->  new GenericConsumer())))
    consumers.foreach(_.start())

    while(leader.hasLeadership) {
      log.info("doing something useful")
      Thread.sleep(10000)
    }
  }

  def logger(serviceName: ServiceName, msg: String, e: Throwable) {
    log.error("Unexpected error in " + serviceName + " handling message: " + msg, e)
  }

  def close() {
    log.info("Relinquishing leadership")
    leader.close()
  }
}

class GenericConsumer extends Service {
  def messageReceived(message: Message) = println(message)
}
