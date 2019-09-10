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

import com.enjoyyin.hive.proxy.jdbc.domain.HiveThriftServerInfo
import com.enjoyyin.hive.proxy.jdbc.rule.Balancer
import com.enjoyyin.hive.proxy.jdbc.domain.ExecuteHQLInfo

/**
 * @author enjoyyin
 */
object DefaultBalancer extends Balancer {
  
  def dealOrNot(balancerInfo: BalancerInfo): String = {
    val availPools = balancerInfo.clientPoolInfos.map(t => t.thriftServerInfo.serverName -> t.inUse.toDouble/t.thriftServerInfo.maxThread.toDouble)
    availPools.minBy(_._2)._1
  }
  
}

case class ThriftClientPoolInfo(inFree: Int, inUse: Int, runningHQLs: Array[ExecuteHQLInfo], thriftServerInfo: HiveThriftServerInfo)

case class BalancerInfo(user: ExecuteHQLInfo, clientPoolInfos: Array[ThriftClientPoolInfo])