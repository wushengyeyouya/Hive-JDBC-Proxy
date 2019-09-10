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

package com.enjoyyin.hive.proxy.jdbc.rule.basic

import com.enjoyyin.hive.proxy.jdbc.rule.LoginValidateRule
import com.enjoyyin.hive.proxy.jdbc.util.Logging
import com.enjoyyin.hive.proxy.jdbc.domain.User
import com.enjoyyin.hive.proxy.jdbc.thrift.ProxySession
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf._
import com.enjoyyin.hive.proxy.jdbc.domain.DealRule._
import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.auth.PasswdAuthenticationProvider
import com.enjoyyin.hive.proxy.jdbc.rule.ThriftServerNameRule
import com.enjoyyin.hive.proxy.jdbc.util.Utils
import javax.security.sasl.AuthenticationException


/**
 * @author enjoyyin
 */
object DefaultLoginValidateRule extends LoginValidateRule with PasswdAuthenticationProvider {
  
  private def validateByUser(user: String, password: String): Boolean = {
    val pwd = getPwdByUser(user)
    pwd.contains(password)
  }
  
  override def dealOrNot(user: User): Boolean = {
    val session = user.asInstanceOf[ProxySession]
    val username = session.username
    if(validateByUser(username, session.password)){
      return true
    }
    val thriftServerName = session.thriftServerName
    val userPwds = getHiveThriftServerInfo(thriftServerName).map(t => t.username -> t.password).toMap
    if(!userPwds.contains(username)) {
      return false
    } else if(StringUtils.isNotEmpty(userPwds(username)) &&
        userPwds(username) != session.password) {
      return false
    }
    true
  }
  
  override def Authenticate(user: String, password: String) : Unit = {
    val thriftServer = Utils.tryAndLogError(ThriftServerNameRule.getThriftServerName(null, user, null))
    if(thriftServer != null) {
      val thriftServerName = thriftServer.thriftServerName
      if(!getThriftNames.contains(thriftServerName)) {
        throw new AuthenticationException("Cannot redirect to correct thrift proxy!")
      }
      val session = new ProxySession(null, thriftServerName, 0,
        thriftServer.username, password, null)
      getHiveThriftProxyRule(thriftServerName).loginValidate match {
        case Default => if(!dealOrNot(session)) throw new AuthenticationException("Wrong username or password!")
        case UserDefine => 
          val canLogin = Utils.tryThrow(getHiveThriftProxyRuleClass(thriftServerName).loginValidateRule.dealOrNot(session))(
            new AuthenticationException("Login authentication has occured an error！", _))
          if(!canLogin) {
            throw new AuthenticationException("Wrong username or password!")
          }
        case _ =>
      }
    } else if(!validateByUser(user, password)){
      throw new AuthenticationException("Wrong username or password!")
    }
  }
  
}