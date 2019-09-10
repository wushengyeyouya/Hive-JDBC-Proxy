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

import java.util.concurrent.BlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import org.apache.hive.service.cli.HiveSQLException
import scala.collection.JavaConversions._

/**
 * @author enjoyyin
 */
abstract class EventLoop[E](override val threadName: String, val maxCapacity: Int) extends LoopThread with LoopCollection[E] {
  private val eventQueue: BlockingQueue[E] = new PriorityBlockingQueue[E](2 * maxCapacity + 1)

  protected override def doLoop = {
    val event = eventQueue.take
    onReceiveSafety(event)
  }

  def remove(obj: E): Unit = {
    eventQueue.remove(obj)
  }
  
  def remove(filter: E => Boolean): Unit = {
    eventQueue.filter(filter).foreach(eventQueue.remove)
  }

  def post(event: E): Unit = {
    if(eventQueue.size > maxCapacity) {
      throw new HiveSQLException(threadName + " have reached max capacity of queue!")
    }
    eventQueue.put(event)
  }

  def leftCapacity = maxCapacity - eventQueue.size

}