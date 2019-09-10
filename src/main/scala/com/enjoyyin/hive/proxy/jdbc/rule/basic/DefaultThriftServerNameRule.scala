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

import com.enjoyyin.hive.proxy.jdbc.rule.ThriftServerNameRule
import com.enjoyyin.hive.proxy.jdbc.rule.ThriftServerNameRule._
import org.apache.commons.lang3.StringUtils
import com.enjoyyin.hive.proxy.jdbc.domain.ThriftServerName


/**
 * @author enjoyyin
 */
object DefaultThriftServerNameRule extends ThriftServerNameRule {
  
  override def dealOrNot(params: Map[String, String]): ThriftServerName = {
    var thriftServerName = ""
    if(params != null && params.contains(THRIFT_CONNECTION_NAME)) {
      thriftServerName = params(THRIFT_CONNECTION_NAME)
    }
    var _username = params(USERNAME_NAME)
    if(StringUtils.isEmpty(thriftServerName)) {
      if(StringUtils.isNotEmpty(_username) && _username.indexOf("_") > 0) {
        val _arrays = _username.split("_")
        if(_arrays.length != 2) {
          throw new IllegalArgumentException(s"非法的用户名${_username}.")
        }
        thriftServerName = _arrays(0)
        _username = _arrays(1)
      } else {
        thriftServerName = _username
      }
      if(StringUtils.isEmpty(thriftServerName)) {      
        throw new NullPointerException(s"JDBC url must have $THRIFT_CONNECTION_NAME")
      }
    }
    ThriftServerName(thriftServerName, _username)
  }
  
  override def canDeal(params: Map[String, String]) = true
}