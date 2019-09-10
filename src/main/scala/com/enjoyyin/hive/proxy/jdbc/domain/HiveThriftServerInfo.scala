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

package com.enjoyyin.hive.proxy.jdbc.domain

import com.enjoyyin.hive.proxy.jdbc.rule.UserAllowRule
import com.enjoyyin.hive.proxy.jdbc.rule.QueryDealRule
import com.enjoyyin.hive.proxy.jdbc.rule.LoginValidateRule
import org.apache.commons.lang3.StringUtils
import com.enjoyyin.hive.proxy.jdbc.rule.StatisticsDealRule
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.MAX_EXECUTE_TIME_OF_OPERATION
import com.enjoyyin.hive.proxy.jdbc.util.Utils
import com.enjoyyin.hive.proxy.jdbc.rule.Balancer

/**
 * @author enjoyyin
 */

object AllowRule extends Enumeration {
  type AllowRule = Value
  val Allow, UserDefine, Error, CancelPrev= Value
}
object DealRule extends Enumeration {
  type DealRule = Value
  val NONE, Default, UserDefine = Value
}
import AllowRule._
import DealRule._
case class HiveThriftServerInfo(thriftServerName: String, serverName: String, valid: Boolean, username: String,
                                password: String, host: String, port: Int, maxThread: Int, loginTimeout: Int = 5000) {
  def validate: Unit = {
    if(StringUtils.isEmpty(host) || !host.matches("[0-9\\.a-zA-Z\\-]{7,}")) {
      throw new IllegalArgumentException(s"${thriftServerName}的host($host)不存在或不合法！")
    }
    if(StringUtils.isEmpty(username)) {
      throw new IllegalArgumentException(s"${thriftServerName}的username不能为空！")
    }
    if(port <= 0) {
      throw new IllegalArgumentException(s"${thriftServerName}的port必须大于0！")
    }
    if(maxThread <= 0 || maxThread >= 50) {
      throw new IllegalArgumentException(s"${thriftServerName}的maxThread必须处于(0, 50]之间！")
    }
    if(loginTimeout < MAX_EXECUTE_TIME_OF_OPERATION) {
      throw new IllegalArgumentException(s"${thriftServerName}的loginTimeout必须大于参数operation.execute.time.max(${Utils.msDurationToString(MAX_EXECUTE_TIME_OF_OPERATION)})！")
    }
  }
}
case class HiveThriftProxyRule(thriftServerName: String, allowMultiSessionsInIP: AllowRule = Allow,
    allowMultiSessionsInUser: AllowRule = Allow, loginValidate: DealRule = NONE,
    queryDeal: DealRule = Default, statisticsDeal: DealRule = Default, balancerRule: DealRule = Default)
 
case class HiveThriftProxyRuleClass(thriftServerName: String, mutiSessionsInIPRule: UserAllowRule = null,
    mutiSessionsInUserRule: UserAllowRule = null, statisticsDealRule: StatisticsDealRule = null,
    loginValidateRule: LoginValidateRule = null, queryDealRule: List[QueryDealRule] = List.empty[QueryDealRule],
    balancer: Balancer = null)
    
case class User(username: String, password: String, ipAddress: String)

case class UserHQL(user: User, hqlPriority: HQLPriority, executeHQLs: Map[String, Array[ExecuteHQLInfo]])

case class HQLPriority(hql: String, priority: Int)

case class ExecuteHQLInfo(username: String, ipAddress: String, hql: String)

case class ThriftServerName(thriftServerName: String, username: String)
