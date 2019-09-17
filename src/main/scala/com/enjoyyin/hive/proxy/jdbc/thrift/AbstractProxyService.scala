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

import org.apache.hive.service.cli.thrift.TSessionHandle
import org.apache.hive.service.cli.HandleIdentifier
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf._
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import org.apache.hive.service.cli.thrift.TOperationHandle
import com.enjoyyin.hive.proxy.jdbc.util.Logging
import com.enjoyyin.hive.proxy.jdbc.domain.User
import com.enjoyyin.hive.proxy.jdbc.exception.ValidateFailedException
import com.enjoyyin.hive.proxy.jdbc.rule.UserAllowRule
import com.enjoyyin.hive.proxy.jdbc.util.Utils
import org.apache.hive.service.cli.thrift.TExecuteStatementReq
import com.enjoyyin.hive.proxy.jdbc.domain.UserHQL
import com.enjoyyin.hive.proxy.jdbc.rule.ThriftServerNameRule
import com.enjoyyin.hive.proxy.jdbc.rule.basic.StatisticsDealListener
import com.enjoyyin.hive.proxy.jdbc.domain.HQLPriority
import com.enjoyyin.hive.proxy.jdbc.util.DaemonThread
import org.apache.commons.lang.StringUtils

/**
 * @author enjoyyin
 */
private[jdbc] trait AbstractProxyService extends Logging {
  private val sessions = new ConcurrentHashMap[TSessionHandle, ProxySession]
  private val userThriftServices = new ConcurrentHashMap[String, UserThriftProxyService]
  private val listenerBus = new ListenerBusEventLoop
  private val cleaner = new DaemonThread("session-release-thread") {
	  val maxHoldTimeString = Utils.msDurationToString(DEFAULT_USER_SESSION_MAX_HOLD_TIME)
    override def doLoop = {
    	  val now = System.currentTimeMillis
        sessions.synchronized(sessions.filter { case (_, pSession) => 
    	    now - pSession.lastAccessTime >= DEFAULT_USER_SESSION_MAX_HOLD_TIME
    	  }.foreach { case (session, sessionProxy) =>
          logWarning(s"Ready to close a session for user(${sessionProxy.username}, ${sessionProxy.ipAddress}), which last access at ${Utils.dateFormat(sessionProxy.lastAccessTime)}, since it has reached max hold time($maxHoldTimeString).")
          closeSession(session)
        })
        Utils.tryIgnoreError(Thread.sleep(60000))
    }
  }

  def init = {
    proxyService = this
    val thriftNames = getThriftNames
    thriftNames.foreach { thriftName =>
      logInfo(s"Init a new thrift user pool of thrift user($thriftName).")
      val userThriftProxyService = new UserThriftProxyService(thriftName)
      userThriftProxyService.start
      userThriftServices += thriftName -> userThriftProxyService
    }
    listenerBus.addListener(new Listener {
      override def onChange(thriftServerName: String, event: ProxyServiceEvent[_]): Unit = {
        val sessionHandle = Utils.invokeAndIgnoreError(event.req, null, "getSessionHandle").asInstanceOf[TSessionHandle]
        if(sessionHandle != null) {
          updateSessionAccessTime(sessionHandle)
          if(event.isInstanceOf[ExecuteStatement]) {
        	  val userThriftService = userThriftServices.get(thriftServerName)
        		if(userThriftService != null) {
        			userThriftService.markExecuteHQLCompleted(sessionHandle)
        		}
          }
        }
      }
    })
    listenerBus.addListener(new StatisticsDealListener)
    listenerBus.start
    cleaner.start
  }

  private[jdbc] def reInitThrift = {
    userThriftServices.synchronized {
      val thriftNames = getThriftNames
      val oldUserThriftNames = userThriftServices.keySet
      oldUserThriftNames.filterNot(thriftNames.contains).foreach { thriftName =>
        sessions.filter(_._2.thriftServerName == thriftName).foreach { case (sessionHandle, _) =>
          closeSession(sessionHandle)
        }
        userThriftServices(thriftName).close
        userThriftServices -= thriftName
      }
      thriftNames.foreach { thriftName =>
        val oldThriftService = userThriftServices.get(thriftName)
        if(oldThriftService == null) {
        	logInfo(s"Init a new thrift user pool of thrift user($thriftName).")
        	val userThriftProxyService = new UserThriftProxyService(thriftName)
        	userThriftProxyService.start
        	userThriftServices += thriftName -> userThriftProxyService
        } else {
          oldThriftService.reInit
        }
      }
    }
  }

  def close = {
    userThriftServices.foreach { case (_, client) =>
      client.close
    }
    userThriftServices.clear
    Utils.tryIgnoreError(cleaner.stop)
    Utils.tryIgnoreError(listenerBus.stop)
    ProxyServiceEventLoop.shutdown
  }

  protected def newSessionHandle : TSessionHandle = {
    new TSessionHandle(new HandleIdentifier().toTHandleIdentifier)
  }

  protected def validateSession(conf: java.util.Map[String, String], username: String,
      ipAddress: String, sessionId: TSessionHandle) : Unit = {
		val thriftServer = ThriftServerNameRule.getThriftServerName(conf, username, ipAddress)
    val thriftServerName = thriftServer.thriftServerName
    if(!userThriftServices.containsKey(thriftServerName)) {
    	throw new ValidateFailedException(s"Cannot redirect to correct thrift proxy!")
    } else if(!userThriftServices(thriftServerName).isValid) {
    	throw new ValidateFailedException(s"Thrift-${thriftServerName}暂时不对外提供服务，更多详细信息，请咨询相关管理员！")
    }
    val session = new ProxySession(sessionId, thriftServerName, System.currentTimeMillis,
        thriftServer.username, null, ipAddress)
    if(conf != null && conf.containsKey(ProxySession.USE_DATABASE)) session.setCurrentDB(conf.get(ProxySession.USE_DATABASE))
    validateProxySession(session)
    sessions += sessionId -> session
    logInfo(s"Open a session for user(${thriftServer.username}, address: $ipAddress), sessionId is: $sessionId")
  }

  import com.enjoyyin.hive.proxy.jdbc.domain.DealRule._
  import com.enjoyyin.hive.proxy.jdbc.domain.AllowRule
  import com.enjoyyin.hive.proxy.jdbc.domain.AllowRule._
  private def validateProxySession(session: ProxySession): Boolean = {
    val rule = getHiveThriftProxyRule(session.thriftServerName)
    val ruleClass = getHiveThriftProxyRuleClass(session.thriftServerName)
    val prevs = sessions.values.filter(session.narrowEquals).toArray
    val now = session
    def ruleResultDeal(rule: AllowRule, ruleClass: UserAllowRule): Unit = {
      rule match {
        case AllowRule.UserDefine =>
          val cancelSessions = Utils.tryCatch(ruleClass.allowOrNot(prevs, now))(t => throw new ValidateFailedException(t))
          if(cancelSessions != null) cancelSessions.foreach(session => closeSession(session.sessionId))
        case Error => throw new ValidateFailedException("错误：同个IP不允许多个用户同时使用！")
        case CancelPrev => prevs.foreach(session => closeSession(session.sessionId))
        case _ =>
      }
    }
    if(!prevs.isEmpty) {
      ruleResultDeal(rule.allowMultiSessionsInIP, ruleClass.mutiSessionsInIPRule)
      ruleResultDeal(rule.allowMultiSessionsInUser, ruleClass.mutiSessionsInUserRule)
    }
    true
  }

  private def validateHQL(session: ProxySession, event: ProxyServiceEvent[_], queryDeal: DealRule): Unit = {
    val thriftServerName = session.thriftServerName
    val req = event.req.asInstanceOf[TExecuteStatementReq]
    val hql = req.getStatement
    queryDeal match {
      case NONE =>
      case _ =>
        val ruleClass = getHiveThriftProxyRuleClass(thriftServerName)
        val executeHQLs = userThriftServices(thriftServerName).getExecuteHQLs
        var tmpHqlPriority = UserHQL(session, HQLPriority(hql, 0), executeHQLs)
        ruleClass.queryDealRule.foreach(d => tmpHqlPriority = UserHQL(session, d.dealOrNot(tmpHqlPriority), executeHQLs))
        val hqlPriority = tmpHqlPriority.hqlPriority
        req.setStatement(hqlPriority.hql)
        event.specificPriority = hqlPriority.priority
    }
    req.getStatement match {
      case ProxySession.USE_DATABASE_REGEX1(db) => session.setCurrentDB(db)
      case ProxySession.USE_DATABASE_REGEX2(db) => session.setCurrentDB(db)
      case ProxySession.USE_DATABASE_REGEX3(db) =>
        session.getCurrentDB.foreach(db => req.setStatement(s"use $db;${req.getStatement}"))
        session.setCurrentDB(db)
      case ProxySession.USE_DATABASE_REGEX4(db) =>
        session.getCurrentDB.foreach(db => req.setStatement(s"use $db;${req.getStatement}"))
        session.setCurrentDB(db)
      case _ =>
    }

  }

  protected def closeSession(sessionId: TSessionHandle) {
    if(sessions.containsKey(sessionId)) {
      val session = sessions(sessionId)
    	val thriftServerName = session.thriftServerName
    	logInfo(s"Close a session for user(${session.username}, ${session.ipAddress})")
    	userThriftServices.synchronized {
    		if(userThriftServices.containsKey(thriftServerName)) {
    			userThriftServices(thriftServerName).cancelSession(sessionId)
    		}
      }
    	sessions -= sessionId
    }
  }

  private[jdbc] def updateSessionAccessTime(sessionId: TSessionHandle): Unit = {
    if(sessions.containsKey(sessionId)) {
      sessions.synchronized {
        if(sessions.containsKey(sessionId)) {
          sessions(sessionId).updateAccessTime
        }
      }
    }
  }


  protected def putEvent(sessionId: TSessionHandle, event: ProxyServiceEvent[_]): Unit = {
    updateSessionAccessTime(sessionId)
    if(sessions.containsKey(sessionId)) {
    	val session = sessions(sessionId)
    	val thriftName = session.thriftServerName
      event.setUser(session)
      listenerBus.post((thriftName, event))
      if(event.isInstanceOf[ExecuteStatement]) {
    	  var validateResult = true
        val rule = getHiveThriftProxyRule(thriftName)
        val req = event.req.asInstanceOf[TExecuteStatementReq]
        Utils.tryCatch(validateHQL(session, event, rule.queryDeal))(t => {
          validateResult = false
          logWarning("验证SQL失败！SQL为：" + req.getStatement, t)
          event.putError(t)
        })
    	  if(!validateResult) return
      }
      userThriftServices.synchronized {
        if(userThriftServices.containsKey(thriftName)) {
          userThriftServices(thriftName).putEvent(event)
        } else {
          logWarning(s"Thrift server $thriftName is not in service!")
          event.putError(s"Thrift server $thriftName is not in service!")
        }
      }
      logDebug("put a new event: " + event.getClass.getSimpleName)
    } else {
      logWarning(s"Cannot find the correct proxy client($sessionId), event is ${event.getClass.getSimpleName}, maybe session has been closed.")
      event.putError("Cannot find the correct proxy client, maybe connection has not been created successfully or already closed.")
    }
  }

  protected def putEvent(opHandle: TOperationHandle, event: ProxyServiceEvent[_]): Unit = {
    var doPut = false
    userThriftServices.synchronized {
      userThriftServices.foreach { case (thriftName, thrift) =>
        if(thrift.containsOperationHandles(opHandle)) {
          listenerBus.post((thriftName, event))
          thrift.putEvent(event)
          doPut = true
          logDebug("put a new event: " + event.getClass.getSimpleName)
        }
      }
    }
    if(!doPut) {
      logWarning(s"Cannot find the corrent proxy client($opHandle), event is ${event.getClass.getSimpleName}.")
      event.putError("Cannot find the corrent proxy client.")
    }
  }

}

private[jdbc] class ProxySession(val sessionId: TSessionHandle, val thriftServerName: String,
                                 var lastAccessTime: Long, override val username: String, override val password: String,
                                 override val ipAddress: String)
      extends User(username, password, ipAddress) {
  private var currentDB: Option[String] = None
  def getCurrentDB: Option[String] = currentDB
  def setCurrentDB(db: String): Unit = if(StringUtils.isNotBlank(db)) currentDB = Some(db)
  def clearCurrentDB(): Unit = currentDB = None
  def updateAccessTime = sessionId.synchronized(lastAccessTime = System.currentTimeMillis)
  override def equals(o: Any): Boolean = {
    if(eq(o.asInstanceOf[AnyRef])) {
      return true
    } else if(o == null || !o.isInstanceOf[ProxySession]) {
      return false
    }
    sessionId == o.asInstanceOf[ProxySession].sessionId
  }
  def narrowEquals(other: ProxySession): Boolean = {
    if(eq(other)) {
      return true
    } else if(other == null) {
      return false
    }
    thriftServerName == other.thriftServerName && username == other.username &&
    ipAddress == other.ipAddress
  }
}
object ProxySession {
  val USE_DATABASE = "use:database"
  val USE_DATABASE_REGEX1 = "\\s*use\\s+([^\\s;]+)\\s*;?".r
  val USE_DATABASE_REGEX2 = "\\s*use\\s+([^\\s;]+)\\s*;.+".r
  val USE_DATABASE_REGEX3 = ".+;\\s*use\\s+([^\\s;]+)\\s*;?".r
  val USE_DATABASE_REGEX4 = ".+;\\s*use\\s+([^\\s;]+)\\s*;.+".r
}