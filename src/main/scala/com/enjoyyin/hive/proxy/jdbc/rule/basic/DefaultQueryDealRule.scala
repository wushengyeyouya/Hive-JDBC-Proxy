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

import com.enjoyyin.hive.proxy.jdbc.rule.QueryDealRule
import com.enjoyyin.hive.proxy.jdbc.exception.ValidateFailedException
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.MAX_LENGTH_OF_HQL
import com.enjoyyin.hive.proxy.jdbc.domain.UserHQL
import com.enjoyyin.hive.proxy.jdbc.domain.HQLPriority
/**
 * @author enjoyyin
 */
class DefaultQueryDealRule extends QueryDealRule {

  
  def dealOrNot(hql: UserHQL): HQLPriority = {
    val sql = hql.hqlPriority.hql
    if(sql.length > MAX_LENGTH_OF_HQL) {
      throw new ValidateFailedException(s"The number of letter for this HQL is reached ${sql.length}, We cannot ignore it.")
    }
    if(sql.trim.matches("cache\\s+table\\s+\\S+")) {
      var tableName = sql.trim.replaceFirst("cache", "").replaceFirst("table", "").trim
//      onCacheTables.synchronized(onCacheTables += tableName)
      return HQLPriority(sql, 1000)
    } //else if(onCacheTables.size > 0){
//      onCacheTables.synchronized(onCacheTables.foreach { t =>
//        if(sql.indexOf(t) > 0) {
//    	    throw new ValidateFailedException(s"Table $t is on cache, it cannot be used until it has been cached.")
//        }
//      })
//    }
    HQLPriority(sql, 0)
  }
  
}