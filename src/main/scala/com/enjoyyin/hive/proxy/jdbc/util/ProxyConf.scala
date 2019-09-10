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

package com.enjoyyin.hive.proxy.jdbc.util

import com.enjoyyin.hive.proxy.jdbc.domain.HiveThriftServerInfo
import com.enjoyyin.hive.proxy.jdbc.domain.HiveThriftProxyRule
import com.enjoyyin.hive.proxy.jdbc.domain.HiveThriftProxyRuleClass
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import java.util.concurrent.TimeUnit
import java.io.File

import org.apache.commons.configuration.XMLConfiguration

import scala.collection.JavaConversions._
import org.apache.commons.configuration.tree.ConfigurationNode
import org.apache.commons.configuration.HierarchicalConfiguration.Node
import com.enjoyyin.hive.proxy.jdbc.domain.AllowRule
import com.enjoyyin.hive.proxy.jdbc.domain.DealRule
import com.enjoyyin.hive.proxy.jdbc.domain.AllowRule._
import com.enjoyyin.hive.proxy.jdbc.domain.DealRule._
import com.enjoyyin.hive.proxy.jdbc.rule.UserAllowRule
import com.enjoyyin.hive.proxy.jdbc.rule.StatisticsDealRule
import com.enjoyyin.hive.proxy.jdbc.rule.LoginValidateRule
import com.enjoyyin.hive.proxy.jdbc.rule.QueryDealRule
import com.enjoyyin.hive.proxy.jdbc.rule.Balancer
import com.enjoyyin.hive.proxy.jdbc.exception.ValidateFailedException
import com.enjoyyin.hive.proxy.jdbc.rule.ThriftServerNameRule

import scala.collection.mutable.ArrayBuffer
import com.enjoyyin.hive.proxy.jdbc.thrift.AbstractProxyService
import ProxyConf._
import java.io.FileInputStream

import org.apache.hadoop.hive.conf.HiveConf
import com.enjoyyin.hive.proxy.jdbc.rule.basic.DefaultQueryDealRule
import org.apache.commons.io.IOUtils

/**
 * @author enjoyyin
 */
object ProxyConf extends Logging{
  
  type JList[T] = scala.collection.mutable.ArrayBuffer[T]
  type JMap[K, V] = scala.collection.mutable.HashMap[K, V]
  
  private val propertyFile = "proxy.properties"
  
  private val props: Properties = {
    val inputStream = getClass.getClassLoader.getResourceAsStream(propertyFile)
    val props = new Properties
    Utils.tryFinally(props.load(inputStream))(IOUtils.closeQuietly(inputStream))
    props
  }
  
  val proxyHost: String = get("proxy.bind.host", "localhost")
  val portNum: Int = getInt("proxy.bind.port", 10000)
  val minWorkerThreads: Int = getInt("proxy.work.thread.min", 0)
  val maxWorkerThreads: Int = getInt("proxy.work.thread.max", 500)
  
  val THRIFT_CONNECTION_NAME = "thrift.name"
  
  private val CLIENT_MAX_FREE_TIME = "client.max.free.time"
  val DEFAULT_CLIENT_MAX_FREE_TIME = getLong(CLIENT_MAX_FREE_TIME, Utils.timeStringAsMs("5m"))
  
  private val QUERY_MAX_HOLD_TIME = "query.max.hold.time"
  val DEFAULT_QUERY_MAX_HOLD_TIME = getLong(QUERY_MAX_HOLD_TIME, Utils.timeStringAsMs("1m"))
  
  val MAX_EXECUTE_TIME_OF_OPERATION = getLong("operation.execute.time.max", Utils.timeStringAsMs("2h"))
  
  private val USER_SESSION_MAX_HOLD_TIME = "user.session.max.hold.time"
  val DEFAULT_USER_SESSION_MAX_HOLD_TIME = {    
	  val value = getLong(USER_SESSION_MAX_HOLD_TIME, MAX_EXECUTE_TIME_OF_OPERATION + 600000)
    if(value <= MAX_EXECUTE_TIME_OF_OPERATION) {
      throw new ValidateFailedException("user.session.max.hold.time must bigger than operation.execute.time.max!")
    }
    value
  }
  
  val WAIT_FOR_CLOSE_MAX_TIME = getLong("query.result.max.hold.time", Utils.timeStringAsMs("5s"))
  
	private val MAX_INIT_FAILED_RECORDS = "client.init.failed.max"
  val DEFAULT_MAX_INIT_FAILED_RECORDS = getInt(MAX_INIT_FAILED_RECORDS, 3)
  
  private val MAX_QUERY_FAILED_RECORDS = "client.query.failed.max"
  val DEFAULT_MAX_QUERY_FAILED_RECORDS = getInt(MAX_QUERY_FAILED_RECORDS, DEFAULT_MAX_INIT_FAILED_RECORDS)
  
  
  val MAX_CAPACITY_OF_USER_THRIFT_QUEUE = getInt("user.thrift.queue.max.size", 10000)
  
  val MAX_EXECUTE_PROXY_CLIENT_POOL_SIZE = getInt("proxy.client.max.pool.size", 300)
  
  val MAX_WAIT_TIME_IN_QUEUE = getLong("user.thrift.queue.wait.time.max", Utils.timeStringAsMs("5m"))
  
  val MAX_LENGTH_OF_HQL = getInt("hql.length.max", 50000)
  
  val DEFAULT_MAX_USEFUL_FAILED_INTERVAL = getLong("client.query.failed.time.range", Utils.timeStringAsMs("2m"))
  
  private[util] val resourceFile = get("thrift.xml.filename", "thrift.xml");
  
  Configuration.loadResources
  
  private[util] val loginFile = get("thrift.login.filename", "login.properties")
  
  private[jdbc] val AUTHENTICATION_OF_CUSTOM = "CUSTOM"
  
  private[jdbc] val AUTHENTICATION_OF_NONE = "NONE"
  
  val authTypeStr = {
    val authentication = HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION
    get(authentication.varname, AUTHENTICATION_OF_NONE)
  }
  
  if(AUTHENTICATION_OF_CUSTOM == authTypeStr) LoginConfiguration.loadResource
  
  if(get("user.thrift.resource.reload", "false").toBoolean) {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("thrift-xml-refresh-thread").build
    val scheduledPool = Executors.newSingleThreadScheduledExecutor(threadFactory)
    Utils.addShutdownHook(scheduledPool.shutdown())
	  scheduledPool.scheduleAtFixedRate(new Runnable {
		  override def run(): Unit = {
			  if(Configuration.isFileChanged) {
				  Utils.tryAndLogError(Configuration.loadResources,
            s"Init $resourceFile occured an error, thrift information can't be changed until it is fixed!")
			  }
        if("CUSTOM" == authTypeStr && LoginConfiguration.isFileChanged) {
          Utils.tryAndLogError(LoginConfiguration.loadResource, s"Init $loginFile occured an error, user information can't be changed until it is fixed!")
        }
	    }
	  }, 5, 1, TimeUnit.MINUTES)
  }
  
  private[jdbc] var proxyService: AbstractProxyService = _
  
  private[util] def reInitThrift = if(proxyService != null) proxyService.reInitThrift
  
  def get(key: String, defaultValue: String): String = get(key, t => t, defaultValue)
  
  private def get[T](key: String, op: String => T, defauleValue: T): T = {
    val value = props.getProperty(key)
    if(StringUtils.isNotEmpty(value)) {
      return op(value)
    }
    defauleValue
  }
  
  def getInt(key: String, defauleValue: Int): Int = {
    get(key, _.toInt, defauleValue)
  }
  
  def getLong(key: String, defauleValue: Long): Long = {
    get(key, _.toLong, defauleValue)
  }
  
  def get(key: String): String = {
    get(key, null)
  }
  
  def containsKey(key: String): Boolean = {
    props.containsKey(key)
  }
  
  def getThriftNames: List[String] = {
    Configuration.getThriftServerNames
  }
  
  def getHiveThriftServerInfo(thriftName: String): List[HiveThriftServerInfo] = {
    Configuration.getThriftResources(thriftName)._1
  }
  
  def getHiveThriftProxyRule(thriftName: String): HiveThriftProxyRule = {
    Configuration.getThriftResources(thriftName)._2
  }
  
  def getHiveThriftProxyRuleClass(thriftName: String): HiveThriftProxyRuleClass = {
    Configuration.getThriftResources(thriftName)._3
  }
  
  def getPwdByUser(user: String): Option[String] = LoginConfiguration.getPwdByUser(user)

}
private[util] object Configuration extends Logging {

  private val thriftResources = new JMap[String, (List[HiveThriftServerInfo], HiveThriftProxyRule, HiveThriftProxyRuleClass)]

  private val file: File = Utils.tryAndLogError({
    val path = getClass.getClassLoader.getResource(ProxyConf.resourceFile).toURI;new File(path)},
    "Can not get the path of $resourceFile, will reload it every time.")
  private var lastModifyDate = if(file != null) file.lastModified else 0l

  private[util] def getThriftResources(thriftServerName: String):
    (List[HiveThriftServerInfo], HiveThriftProxyRule, HiveThriftProxyRuleClass) = {
      thriftResources.synchronized(if(thriftResources.contains(thriftServerName)) return thriftResources(thriftServerName))
      null
  }

  private[util] def getThriftServerNames: List[String] = {
	  thriftResources.synchronized(thriftResources.keysIterator.toList)
  }

  private[util] def loadResources: Unit = {
    val thriftResources = init
    if(thriftResources.nonEmpty && Configuration.thriftResources.toString != thriftResources.toString) {
      Configuration.thriftResources.synchronized {
        if(Configuration.thriftResources.toString != thriftResources.toString) {
          logInfo("Thrift Resources info:" + thriftResources)
          Configuration.thriftResources.clear
          Configuration.thriftResources ++= thriftResources
          ProxyConf.reInitThrift
        }
      }
    }
  }

  private[util] def isFileChanged : Boolean = {
    if(file == null) {
      return true
    }
    if(file.lastModified > lastModifyDate || lastModifyDate == 0) {
      lastModifyDate = file.lastModified
      true
    } else {
      false
    }
  }

  private def init : JMap[String, (List[HiveThriftServerInfo], HiveThriftProxyRule, HiveThriftProxyRuleClass)] = {
    val thriftResources = new JMap[String, (List[HiveThriftServerInfo], HiveThriftProxyRule, HiveThriftProxyRuleClass)]
    val waitToRegisterThriftServerNameRules = ArrayBuffer[String]()
    val config = new XMLConfiguration(ProxyConf.resourceFile)
    val root = config.getRoot
    if(root.hasChildren) {
      root.getChildren.foreach { node =>
        val child = node.asInstanceOf[Node]
        var thriftName: String = null
        child.getAttributes.foreach { attr : ConfigurationNode =>
          val value = attr.getValue.asInstanceOf[String]
          attr.getName match {
            case "name" => thriftName = value
          }
        }
        if(StringUtils.isEmpty(thriftName)) {
          throw new IllegalArgumentException(s"Thrift($thriftName)必须有name属性值!")
        } else if(thriftResources.contains(thriftName)) {
          throw new IllegalArgumentException(s"Thrift($thriftName)值出现了重复!")
        }
        var serverInfos: JList[HiveThriftServerInfo] = new JList[HiveThriftServerInfo]
        var allowMultiSessionsInIP: AllowRule = Allow
        var allowMultiSessionsInUser: AllowRule = Allow
        var loginValidate: DealRule = NONE
        var queryDeal: DealRule = Default
        var statisticsDeal: DealRule = Default
        var balancerRule: DealRule = Default
        var multiSessionsInIPRule: UserAllowRule = null
        var multiSessionsInUserRule: UserAllowRule = null
        var statisticsDealRule: StatisticsDealRule = null
        var loginValidateRule: LoginValidateRule = null
        var queryDealRule: Array[QueryDealRule] = Array.empty[QueryDealRule]
        var balancer: Balancer = null
        child.getChildren.foreach { n: ConfigurationNode =>
          n.getName match {
            case "server" =>
              var serverName: String = null
              var valid: Boolean = true
              var username: String = null
              var password: String = null
              var host: String = null
              var port: Int = 0
              var maxThread: Int = 0
              var loginTimeout: Int = ProxyConf.MAX_EXECUTE_TIME_OF_OPERATION.toInt
              n.getAttributes.foreach { attr: ConfigurationNode =>
                val value = attr.getValue.asInstanceOf[String]
                attr.getName match {
                  case "name" => serverName = value
                  case "valid" => valid = value.toBoolean
                  case "username" => username = value
                  case "password" => password = value
                  case "host" => host = value
                  case "port" => port = value.toInt
                  case "maxThread" => maxThread = value.toInt
                  case "loginTimeout" => loginTimeout = Utils.timeStringAsMs(value).toInt
                  case _ =>
                }
              }
              if(StringUtils.isEmpty(serverName)) {
                throw new IllegalArgumentException(s"Thrift($thriftName)的server必须有name属性值!")
              }
              serverInfos += HiveThriftServerInfo(thriftName, serverName, valid, username, password, host, port, maxThread, loginTimeout)
            case "rule" =>
              val name = n.getAttributes.find(_.getName == "name").get
              val ruleName = name.asInstanceOf[ConfigurationNode].getValue.asInstanceOf[String]
              var ruleTypeStr: String = null
              var ruleClassStr: String = ""
              n.getChildren.foreach { rule =>
                val value = rule.getValue.asInstanceOf[String]
                rule.getName match {
                  case "type" =>
                    ruleTypeStr = value
                  case "class" =>
                    ruleClassStr = value + " "
                  case _ =>
                }
              }
              ruleClassStr = ruleClassStr.trim
              def allowRuleInit(rule: AllowRule => Unit, ruleClass: UserAllowRule => Unit): Unit = {
                val ruleType = AllowRule.withName(ruleTypeStr)
                rule(ruleType)
                if(ruleType == AllowRule.UserDefine) {
                  if(StringUtils.isEmpty(ruleClassStr)) {
                    throw new IllegalArgumentException("当使用用户自定义规则时，必须指定相应的实现类！")
                  }
                  ruleClass(Class.forName(ruleClassStr).newInstance.asInstanceOf[UserAllowRule])
                }
              }
              def dealRuleInit(rule: DealRule => Unit, ruleClass: com.enjoyyin.hive.proxy.jdbc.rule.DealRule[_, _] => Unit): Unit = {
                val ruleType = DealRule.withName(ruleTypeStr)
                rule(ruleType)
                if(ruleType == DealRule.UserDefine) {
                  if(StringUtils.isEmpty(ruleClassStr)) {
                    throw new IllegalArgumentException("当使用用户自定义规则时，必须指定相应的实现类！")
                  }
                  ruleClass(Class.forName(ruleClassStr).newInstance.asInstanceOf[com.enjoyyin.hive.proxy.jdbc.rule.DealRule[_, _]])
                }
              }
              ruleName match {
                case "allowMultiSessionsInIP" =>
                  allowRuleInit(allowMultiSessionsInIP = _, multiSessionsInIPRule = _)
                case "allowMultiSessionsInUser" =>
                  allowRuleInit(allowMultiSessionsInUser = _, multiSessionsInUserRule = _)
                case "loginValidate" =>
                  dealRuleInit(loginValidate = _, d => loginValidateRule = d.asInstanceOf[LoginValidateRule])
                case "queryDeal" =>
                  queryDeal = DealRule.withName(ruleTypeStr)
                  if(queryDeal == DealRule.UserDefine) {
                    if(StringUtils.isEmpty(ruleClassStr)) {
                      throw new IllegalArgumentException("当使用用户自定义规则时，必须指定相应的实现类！")
                    }
                    val ruleClasses = ruleClassStr.split(" ")
                    queryDealRule = ruleClasses.map {
                      case "Default" => new DefaultQueryDealRule
                      case clazz =>Class.forName(clazz).newInstance.asInstanceOf[QueryDealRule]
                    }
                  } else if(queryDeal == DealRule.Default) {
                    queryDealRule = Array[QueryDealRule]{new DefaultQueryDealRule}
                  }
                case "statisticsDeal" =>
                  dealRuleInit(statisticsDeal = _, d => statisticsDealRule = d.asInstanceOf[StatisticsDealRule])
                case "thriftNameRule" => waitToRegisterThriftServerNameRules += ruleClassStr
                case "balancer" => dealRuleInit(balancerRule = _, d => balancer = d.asInstanceOf[Balancer])
                case r: String => logWarning(s"Ignore not exist rule $r")
              }
          }
        }
        if(serverInfos.isEmpty) {
          throw new IllegalArgumentException(s"thrift $thriftName 不存在server配置信息!")
        }
        serverInfos.foreach(_.validate)
        if(serverInfos.map(_.serverName).distinct.size != serverInfos.size) {
          throw new IllegalArgumentException(s"thrift $thriftName 存在重复的server name属性值!")
        }
        val rule = HiveThriftProxyRule(thriftName, allowMultiSessionsInIP, allowMultiSessionsInUser, loginValidate,
            queryDeal, statisticsDeal, balancerRule)
        val ruleClass = HiveThriftProxyRuleClass(thriftName, multiSessionsInIPRule, multiSessionsInUserRule,
            statisticsDealRule, loginValidateRule, queryDealRule.toList, balancer)
        thriftResources += thriftName -> (serverInfos.toList, rule, ruleClass)
      }
      ThriftServerNameRule.register(waitToRegisterThriftServerNameRules.toArray)
    }
    thriftResources
  }

}

private[util] object LoginConfiguration extends Logging {
  private val file: File = Utils.tryAndLogError({
    val path = getClass.getClassLoader.getResource(ProxyConf.loginFile).toURI
    new File(path)
  }, "Can not get the path of $resourceFile, will reload it every time.")
  private var lastModifyDate = if(file != null) file.lastModified else 0l

  private val userToPwd = new JMap[String, String]

  private[util] def getPwdByUser(user: String): Option[String] =
    userToPwd synchronized userToPwd.get(user)

  private[util] def isFileChanged: Boolean = {
    if(file == null) {
      return true
    }
    if(file.lastModified > lastModifyDate || lastModifyDate == 0) {
      lastModifyDate = file.lastModified
      true
    } else false
  }

  private[util] def loadResource = {
      val userToPwd = load
      if(userToPwd != null && userToPwd != LoginConfiguration.userToPwd) {
        LoginConfiguration.userToPwd.synchronized {
          if(userToPwd != LoginConfiguration.userToPwd) {
            logInfo("Load login information for user and password => " + userToPwd)
            LoginConfiguration.userToPwd.clear
            LoginConfiguration.userToPwd ++= userToPwd
          }
        }
      }
  }
  
  private def load: JMap[String, String] = {
    val props = new Properties
    val inputStream = new FileInputStream(file)
    Utils.tryFinally(props.load(inputStream))(IOUtils.closeQuietly(inputStream))
    val userToPwd = new JMap[String, String]
    userToPwd ++= props
    userToPwd
  }
}