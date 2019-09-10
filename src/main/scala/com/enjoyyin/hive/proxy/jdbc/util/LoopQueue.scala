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

import org.apache.hive.service.cli.HiveSQLException

/**
 * @author enjoyyin
 */
abstract class LoopQueue[T](override val threadName: String, val maxCapacity: Int) extends LoopThread with LoopCollection[T] {
  private val eventQueue: Array[T] = new Array[Any](maxCapacity).asInstanceOf[Array[T]]
  
  protected override def doLoop: Unit = {
    val tailNum = if(head <= tail) tail else maxCapacity + tail
    for(i <- head to tailNum if head != tail) {
      val index = i % maxCapacity
      val event = eventQueue(index)
      if(event != null && canReceive(event.asInstanceOf[T])) {
        onReceiveSafety(event)
        eventQueue(index) = null.asInstanceOf[T]
        if(index == head) {
          head = index + 1
        }
      } else if(event == null && index == head) {
        head = index + 1
      }
    }
    Utils.tryIgnoreError(Thread.sleep(2000))
  }
  
  private var head = 0
  private var tail = 0
  
  def post(event: T): Unit = {
    if(head == tail && head != 0 && eventQueue(tail) != null) {
      throw new HiveSQLException(threadName + " have reached max capacity of queue!")
    }
    var stopped = false
    eventQueue.synchronized {
    	val begin = tail
    			for (i <- begin until maxCapacity + begin - 1 if !stopped) {
    				val index = i % maxCapacity
    				  if(eventQueue(index) == null) {
    					  eventQueue(index) = event
    						tail = (index + 1) % maxCapacity
    						stopped = true
    					}
    			}
    }
    if(!stopped) {
      throw new HiveSQLException(threadName + " have reached max capacity of queue!")
    }
  }
  
  def remove(obj: T): Unit = throw new UnsupportedOperationException
  
  def remove(filter: T => Boolean): Unit = throw new UnsupportedOperationException
  
  protected def canReceive(event: T): Boolean
  
}