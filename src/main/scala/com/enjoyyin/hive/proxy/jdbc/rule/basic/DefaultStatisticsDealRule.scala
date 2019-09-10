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

import com.enjoyyin.hive.proxy.jdbc.rule.StatisticsDealRule
import com.enjoyyin.hive.proxy.jdbc.thrift.EventInfo
import com.enjoyyin.hive.proxy.jdbc.thrift.Listener
import com.enjoyyin.hive.proxy.jdbc.util.Logging
import com.enjoyyin.hive.proxy.jdbc.util.Utils
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf._
import com.enjoyyin.hive.proxy.jdbc.thrift.ProxyServiceEvent
import com.enjoyyin.hive.proxy.jdbc.domain.DealRule._
import org.apache.commons.lang3.StringUtils

/**
 * @author enjoyyin
 */
object DefaultStatisticsDealRule extends StatisticsDealRule with Logging {
  
  override def dealOrNot(eventInfo: EventInfo): Unit = {
    if(StringUtils.isEmpty(eventInfo.hql)) return
    val executeTime = eventInfo.finishedExecuteTime - eventInfo.startExecuteTime
    val waitTime = eventInfo.startExecuteTime - eventInfo.startWaitTime
    val msg = new StringBuilder
    msg ++= "Since the beginning waiting time is " ++ Utils.dateFormat(eventInfo.startWaitTime) ++ 
      ", I who come from (name=" ++ eventInfo.user.username ++ ", ip=" ++ eventInfo.user.ipAddress ++ "), have cost " ++ Utils.msDurationToString(waitTime) ++ " waiting for execute, and the beginning executing time is " ++
      Utils.dateFormat(eventInfo.startExecuteTime) ++ "; have cost " ++
      Utils.msDurationToString(executeTime) ++ " to be completed, when the finish time is " ++
      Utils.dateFormat(eventInfo.finishedExecuteTime) ++
      ", and the final state is " ++ eventInfo.state.toString ++ ", hql is: " ++ eventInfo.hql
    if(StringUtils.isNotEmpty(eventInfo.errorMsg)) {
      msg ++= ", and error message is: " ++ eventInfo.errorMsg
    }
    logInfo(msg.toString)
  }
}

class StatisticsDealListener extends Listener {
  
  def onChange(thriftServerName: String, event: ProxyServiceEvent[_]): Unit = {
    val eventInfo = event.getEventInfo
    eventInfo.foreach { e =>
      getHiveThriftProxyRule(thriftServerName).statisticsDeal match {
        case Default => DefaultStatisticsDealRule.dealOrNot(e)
        case UserDefine => getHiveThriftProxyRuleClass(thriftServerName).statisticsDealRule.dealOrNot(e)
        case _ =>
      }
    }
  }
  
}
