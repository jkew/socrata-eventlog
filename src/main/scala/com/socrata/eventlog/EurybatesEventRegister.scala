package com.socrata.eventlog

import com.netflix.curator.framework.CuratorFramework
import com.twitter.logging.Logger
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import com.netflix.curator.framework.recipes.leader.LeaderLatch

/**
 * Registers to listen for events.
 *
 * Test:
 *  - registers as single node
 *  - deregisters on exit
 *  - second node says we are already registered
 *  - second node exit
 */
class EurybatesEventRegister(client:CuratorFramework) {
  val log:Logger = Logger.get(this.getClass)
  val service = "eventlog"
  val zkLeaderPath = "/eventlog/leader"
  val zkEurybatesPath =  "/eurybates/services/" + service
  val leader = new LeaderLatch(client, zkLeaderPath)

  def stayRegistered = {
    leader.start()
    log.info("Waiting for leadership.")
    leader.await()
    log.info("I am the leader; I am responsible for registering with eurybates!")
    if (!register) {
      log.error("Unable to register with eurybates; Abandoning ship!")
    } else {
      log.info("Registered successfully; waiting for godot")
      while (leader.hasLeadership) {
        Thread.sleep(10000)
      }
    }
  }

  def register:Boolean = {
    log.info("Registering with Eurybates")
    try {
      try {
        client.delete().forPath(zkEurybatesPath)
      } catch {
        case nne:NoNodeException => log.info("No previous eventlog was registered")
        case e:Exception => log.error("Error removing previous registration", e)
      }
      client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(zkEurybatesPath, leader.getId.getBytes())
      log.info("Registered!")
      true
    } catch {
      case ne:NodeExistsException => log.info("Someone already registered with eurybates"); leader.close(); true
      case e:Exception =>
        log.error("Unable to register with eurybates!", e)
        return false
    }

  }

  def close {
    leader.close()
  }

}