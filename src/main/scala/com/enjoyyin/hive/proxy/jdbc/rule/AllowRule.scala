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

package com.enjoyyin.hive.proxy.jdbc.rule

import com.enjoyyin.hive.proxy.jdbc.thrift.ProxySession
import com.enjoyyin.hive.proxy.jdbc.domain.User
import com.enjoyyin.hive.proxy.jdbc.thrift.EventInfo
import com.enjoyyin.hive.proxy.jdbc.domain.UserHQL
import com.enjoyyin.hive.proxy.jdbc.domain.ThriftServerName
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf
import java.util.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet
import com.enjoyyin.hive.proxy.jdbc.rule.basic.DefaultThriftServerNameRule
import com.enjoyyin.hive.proxy.jdbc.util.Logging
import com.enjoyyin.hive.proxy.jdbc.domain.HQLPriority
import com.enjoyyin.hive.proxy.jdbc.rule.basic.BalancerInfo


/**
 * @author enjoyyin
 */
abstract class UserAllowRule {
  
  /**
   * 用户自定义的实现类，返回需要被取消的session列表
   */
  def allowOrNot(prev: Array[ProxySession], now: ProxySession): Array[ProxySession]
  
}

private[jdbc] abstract class DealRule[T, K] {
  
  def dealOrNot(t: T): K
  
}

abstract class LoginValidateRule extends DealRule[User, Boolean] {
  
  /**
   * 判断用户名、密码是否合法
   */
  override def dealOrNot(user: User): Boolean
  
}

abstract class QueryDealRule extends DealRule[UserHQL, HQLPriority] {
  
  /**
   * 对SQL进行加工处理，
   * 返回一个最后将会被实际运行的SQL
   */
  override def dealOrNot(hql: UserHQL): HQLPriority
  
}

abstract class StatisticsDealRule extends DealRule[EventInfo, Unit] {
  
  override def dealOrNot(eventInfo: EventInfo): Unit
  
}

abstract class Balancer extends DealRule[BalancerInfo, String] {
  
  def dealOrNot(balancerInfo: BalancerInfo): String

}

abstract class ThriftServerNameRule extends DealRule[Map[String, String], ThriftServerName] {
  
  /**
   * 从已知的参数中，通过相关算法，指向一个thriftServer，返回该thriftServerName和对应该session的userName。<br>
   * params包含我们内嵌的2个参数username、ipAddress，但是也包含所有从jdbc client传递过来的参数信息（比如我们默认的thrift.name参数名，
   * 用于指定thriftServerName），用户可以通过这些参数算出thriftServerName和userName。<br>
   */
  override def dealOrNot(params: Map[String, String]): ThriftServerName
  
  def canDeal(params: Map[String, String]): Boolean
}

object ThriftServerNameRule extends Logging{
  val THRIFT_CONNECTION_NAME = ProxyConf.THRIFT_CONNECTION_NAME
  val USERNAME_NAME = "username"
  val IPADDRESS_NAME = "ipAddress"
  
  type JMap[K, V] = java.util.Map[K, V]
  
	private val registeredRules: HashSet[ThriftServerNameRule] = HashSet[ThriftServerNameRule]()
  
  private def toParamsMap(conf: JMap[String, String], username: String, ipAddress: String): Map[String, String] = {
    var params = conf
    if(conf == null) {
      params = new HashMap[String, String]
    }
    params += USERNAME_NAME -> username
    params += IPADDRESS_NAME -> ipAddress
    params.toMap
  }
  
  private def register(ruleName: String): Unit = {
    val ruleClass = Class.forName(ruleName).newInstance.asInstanceOf[ThriftServerNameRule]
    registeredRules.synchronized(registeredRules += ruleClass)
    logInfo("Registered a thrift-server-name-rule " + ruleName)
  }
  
  def register(ruleNames: Array[String]): Unit = {
    if(ruleNames.isEmpty) return
    registeredRules.synchronized {
      registeredRules.clear
      ruleNames.foreach(register)
    }
  }
  
  def getThriftServerName(conf: JMap[String, String], username: String, ipAddress: String): ThriftServerName = {
    val params = toParamsMap(conf, username, ipAddress)
    var rule = registeredRules.synchronized(registeredRules.find(_.canDeal(params)))
    if(rule.isEmpty) {
      rule = Some(DefaultThriftServerNameRule)
    }
    rule.get.dealOrNot(params)
  }
}