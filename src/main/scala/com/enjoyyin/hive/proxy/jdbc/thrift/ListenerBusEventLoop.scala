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

import com.enjoyyin.hive.proxy.jdbc.util.{Logging, LoopQueue, Utils}
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf._

import scala.util.control.NonFatal

/**
 * @author enjoyyin
 */
private[thrift] class ListenerBusEventLoop extends LoopQueue[(String, ProxyServiceEvent[_])]("listener-deal-event-loop", MAX_CAPACITY_OF_USER_THRIFT_QUEUE/2) {
  
  private val listeners: JList[Listener] = new JList[Listener]()
  
  private[thrift] def addListener(listener: Listener) {
    listeners += listener
  }
  
  override def onError(e: Throwable): Unit = {
    logWarning("", e)
  }
  
  override protected def onReceive(event: (String, ProxyServiceEvent[_])): Unit = {
    val iter = listeners.iterator
    while (iter.hasNext) {
      val listener = iter.next()
      Utils.tryCatch {
        listener.onChange(event._1, event._2)
      } { case NonFatal(e) =>
        logError(s"Listener ${listener.getClass.getSimpleName} threw an exception!", e)
      }
    }
  }
  
  override protected def canReceive(event: (String, ProxyServiceEvent[_])): Boolean = {
    event._2.isCompleted
  }
  
}

trait Listener extends Logging {
  
  def onChange(thriftServerName: String, event: ProxyServiceEvent[_]): Unit
  
}