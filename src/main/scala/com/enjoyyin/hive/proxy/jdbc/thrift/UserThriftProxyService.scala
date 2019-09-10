/*
 *
 *  * Copyright 2019 enjoyyin
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.enjoyyin.hive.proxy.jdbc.thrift

import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf._
import org.apache.thrift.TBase
import com.enjoyyin.hive.proxy.jdbc.util.Logging
import org.apache.hive.service.cli.thrift.TOperationHandle
import org.apache.hive.service.cli.thrift.TSessionHandle
import com.enjoyyin.hive.proxy.jdbc.util.Utils
import java.util.concurrent.atomic.AtomicLong

import com.enjoyyin.hive.proxy.jdbc.domain.HiveThriftServerInfo
import org.apache.hive.service.cli.HiveSQLException
import com.enjoyyin.hive.proxy.jdbc.domain.DealRule._
import com.enjoyyin.hive.proxy.jdbc.rule.basic.ThriftClientPoolInfo
import com.enjoyyin.hive.proxy.jdbc.rule.basic.DefaultBalancer
import org.apache.hive.service.cli.thrift.TExecuteStatementReq
import com.enjoyyin.hive.proxy.jdbc.domain.ExecuteHQLInfo
import com.enjoyyin.hive.proxy.jdbc.rule.Balancer
import com.enjoyyin.hive.proxy.jdbc.rule.basic.BalancerInfo
import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.cli.thrift.THandleIdentifier
import com.enjoyyin.hive.proxy.jdbc.util.DaemonThread

/**
 * @author enjoyyin
 */
private[thrift] class UserThriftProxyService(val thriftName: String) extends Logging {
  private var thriftServerInfos = getHiveThriftServerInfo(thriftName)
  private var balancer = initBalancer
	private val thriftPools = new JMap[String, ThriftClientPool]
  private val executeHQLs = new JMap[String, JList[ExecuteHQLInfo]]
  private val sessionToExecuteHQLs = new JMap[THandleIdentifier, (String, ExecuteHQLInfo)]
  private val eventLoop = new ProxyServiceEventLoop(this)
  private val releaseInterval = 2000

  private[thrift] def getClient(event: ProxyServiceEvent[_]): ClientProxy = {
    val req = event.req
    val isExecuteStatement = event.isInstanceOf[ExecuteStatement]
    val sessionHandle = Utils.invokeAndIgnoreError(req, null, "getSessionHandle").asInstanceOf[TSessionHandle]
    def getExecuteHQLInfo: ExecuteHQLInfo = {
      val user = event.getUser
      val hql = if(isExecuteStatement) req.asInstanceOf[TExecuteStatementReq].getStatement else null
      ExecuteHQLInfo(user.username, user.ipAddress, hql)
    }
    val opHandle = Utils.invokeAndDealError(req, null, "getOperationHandle", t => t match {
      case _: NoSuchMethodException =>
      case e => throw e
    }).asInstanceOf[TOperationHandle]
    if(opHandle != null) {
      thriftPools.synchronized {
        val thriftPool = thriftPools.find(_._2.containsOperationHandles(opHandle))
        if(thriftPool.isDefined) {
          return thriftPool.get._2.getClient(req)
        } else {
          throw new HiveSQLException(s"Cannot find the corrent proxy client for thrift user($thriftName), event is ${req.getClass.getSimpleName}.")
        }
      }
    }
    //进入负载均衡部分
    thriftPools.synchronized {
      if(!thriftPools.exists(_._2.getFreeSize > 0)) {
        return null
      }
      val clientPoolInfos = thriftPools.filter(_._2.isValid).toArray.map {case (serverName, thriftPool) =>
        ThriftClientPoolInfo(thriftPool.getFreeSize, thriftPool.getQuerySize, executeHQLs(serverName).toArray, thriftPool.thriftServerInfo)
      }
      if(clientPoolInfos.isEmpty) {
        throw new HiveSQLException(s"Thrift user($thriftName) cannot be connected, for more information, please ask admin for help!")
      }
      val executeHQLInfo = getExecuteHQLInfo
      val serverName: String = balancer.dealOrNot(BalancerInfo(executeHQLInfo, clientPoolInfos))
      if(StringUtils.isNotEmpty(serverName) && thriftPools.contains(serverName)) {
        if(isExecuteStatement) {
          executeHQLs(serverName) += executeHQLInfo
          sessionToExecuteHQLs += sessionHandle.getSessionId -> (serverName, executeHQLInfo)
        }
    	  return thriftPools(serverName).getClient(req)
      } else if(StringUtils.isNotEmpty(serverName) && !thriftPools.contains(serverName)) {
        throw new HiveSQLException(s"Thrift user($thriftName) has a bad balancer, which redirect to a not exist thrift server($serverName) for your request.")
      }
    }
    null
  }

  def isValid: Boolean = thriftPools.exists(_._2.isValid)

  def containsOperationHandles(opHandle: TOperationHandle): Boolean = {
    thriftPools.exists(_._2.containsOperationHandles(opHandle))
  }

  private val releaseThread = new DaemonThread(thriftName + "-release-client-scan_thread") {
	  var lastRetryConnectTime = System.currentTimeMillis
    override def doLoop = {
      Utils.tryIgnoreError(Thread.sleep(releaseInterval))
        thriftPools.synchronized {
          thriftPools.foreach { case (_, thriftPool) =>
            thriftPool.freeClient
            if(System.currentTimeMillis - lastRetryConnectTime >= 60 * releaseInterval &&
                thriftPool.thriftServerInfo.valid && thriftPool.existsNotValid) {
              logWarning(s"Thrift server pool ${thriftPool.serverName} of user $thriftName cannot be connected, now retry to connect it...")
              lastRetryConnectTime = System.currentTimeMillis
              thriftPool.retryConnect
            }
          }
        }
    }
  }

  def reInit: Unit = {
		val newThriftServerInfos = getHiveThriftServerInfo(thriftName)
    if(newThriftServerInfos == thriftServerInfos) return
    thriftPools.synchronized {
      if(newThriftServerInfos == thriftServerInfos) return
      val newThriftServerNames = newThriftServerInfos.map(_.serverName)
      val oldThriftServerNames = thriftPools.keySet
      val newThriftServerNameToInfos = newThriftServerInfos.map(t => t.serverName -> t).toMap
      oldThriftServerNames.filterNot(newThriftServerNames.contains).foreach { serverName =>
    	  thriftPools(serverName).clear
    	  thriftPools -= serverName
        executeHQLs -= serverName
        sessionToExecuteHQLs.filter(_._2._1 == serverName).foreach{ case (session, _) => sessionToExecuteHQLs -= session }
    	}
    	oldThriftServerNames.filter(newThriftServerNames.contains).filter(s => thriftPools(s).thriftServerInfo != newThriftServerNameToInfos(s))
    	  .foreach { serverName =>
      	thriftPools(serverName).clear
        thriftPools -= serverName
        executeHQLs(serverName).clear
        sessionToExecuteHQLs.filter(_._2._1 == serverName).foreach{ case (session, _) => sessionToExecuteHQLs -= session }
      	logInfo(s"ReInit a thrift server pool of thrift user($thriftName) in server($serverName).")
      	val thriftPool = new ThriftClientPool(newThriftServerNameToInfos(serverName))
      	thriftPools += serverName -> thriftPool
    	}
    	newThriftServerNames.filterNot(oldThriftServerNames.contains).foreach { serverName =>
      	logInfo(s"Init a new thrift server pool of thrift user($thriftName) in server($serverName).")
      	val thriftPool = new ThriftClientPool(newThriftServerNameToInfos(serverName))
      	thriftPools += serverName -> thriftPool
        executeHQLs += serverName -> new JList[ExecuteHQLInfo]
    	}
      thriftServerInfos = newThriftServerInfos
      balancer = initBalancer
    }
  }

  def start: Unit = {
		thriftServerInfos.foreach { t =>
      logInfo(s"Init a new thrift server pool of thrift user($thriftName) in server(${t.serverName}).")
      val thriftPool = new ThriftClientPool(t)
      thriftPools += t.serverName -> thriftPool
      executeHQLs += t.serverName -> new JList[ExecuteHQLInfo]
    }
    eventLoop.start
    releaseThread.start
  }

  private[thrift] def getExecuteHQLs = executeHQLs.map(e => e._1 -> e._2.toArray).toMap

  private[thrift] def markExecuteHQLCompleted(session: TSessionHandle): Unit = {
    val sessionId = session.getSessionId
    if(sessionToExecuteHQLs.contains(sessionId)) {
      thriftPools.synchronized {
        if(sessionToExecuteHQLs.contains(sessionId)) {
          val (serverName, executeHQLInfo) = sessionToExecuteHQLs(sessionId)
          executeHQLs(serverName) -= executeHQLInfo
          sessionToExecuteHQLs -= sessionId
        }
      }
    }
  }

  def putEvent(event: ProxyServiceEvent[_]): Unit = {
    eventLoop.post(event)
  }

  def cancelSession(session: TSessionHandle): Unit = {
    eventLoop.removesInQueue(session)
  }

  def close: Unit = {
    Utils.tryIgnoreError(releaseThread.stop)
    Utils.tryIgnoreError(eventLoop.stop)
    thriftPools.foreach(_._2.clear)
    executeHQLs.clear
  }

  private def initBalancer: Balancer = {
    getHiveThriftProxyRule(thriftName).balancerRule match {
      case UserDefine => getHiveThriftProxyRuleClass(thriftName).balancer
      case _ => DefaultBalancer
    }
  }

  private class ThriftClientPool(val thriftServerInfo: HiveThriftServerInfo) {
    private val allocatedNums = new AtomicLong(0)
    private val thriftClientPools = Array.fill(thriftServerInfo.maxThread)(new ClientProxy(thriftServerInfo, allocatedNums))

    val serverName: String = thriftServerInfo.serverName

    def getFreeSize: Int = thriftClientPools.count(_.isFree)

    def getQuerySize: Int = capacity - getFreeSize

    val capacity: Int = thriftServerInfo.maxThread

    def getClient(req: TBase[_, _]): ClientProxy = thriftClientPools.find(_.isAvailable(req)).orNull

    def isValid: Boolean = thriftServerInfo.valid && thriftClientPools.exists(_.isValid)

    def existsNotValid: Boolean = thriftClientPools.exists(!_.isValid)
    
    def retryConnect: Unit = {
      val notValidPools = thriftClientPools.filterNot(_.isValid)
      logWarning(s"Wait to reconnect list of thrift server pool $serverName in user $thriftName is: " + notValidPools.map(_.getClientName).mkString(","))
      val thriftClient = notValidPools(0)
      if(thriftClient.tryToValid) {
        logWarning(s"Retry to connect to thrift server pool $serverName of user $thriftName succeed, now reInit all servers.")
        notValidPools.indices.foreach(thriftClientPools(_).tryToValid)
      } else {
        logWarning(s"Retry to connect to thrift server pool $serverName of user $thriftName failed, wait for next retry.")
      }
    }
    
    def containsOperationHandles(opHandle: TOperationHandle): Boolean = {
      thriftClientPools.exists(_.getOperationHandle.exists(_ == opHandle))
    }
    
    def freeClient: Unit = {
      thriftClientPools.foreach { t =>
        t.release
        t.free
      }
    }
    
    def clear: Unit = {
      thriftClientPools.foreach(_.cancelOperation)
      thriftClientPools.foreach(_.clear)
    }
  }

}
