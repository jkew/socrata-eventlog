package com.socrata.eventlog

import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.recipes.leader.LeaderLatch
import com.twitter.logging.Logger

/**
 * Takes events off a eurybates queue and puts them into a backend
 */
class EventLogProcessor(client:CuratorFramework) extends Runnable {
  val log:Logger = Logger.get(this.getClass)
  val zkPath = "/eventlog/leader"
  val leader = new LeaderLatch(client, zkPath)

  def run() {
    leader.start()
    leader.await() // block forever until we are a leader
    log.info("I am the leader!")
    while(leader.hasLeadership) {
      log.info("doing something useful")
      Thread.sleep(10000)
    }
  }

  def close() {
    log.info("Relinquishing leadership")
    leader.close()
  }
}
