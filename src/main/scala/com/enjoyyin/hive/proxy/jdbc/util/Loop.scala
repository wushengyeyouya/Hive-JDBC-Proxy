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

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal
import com.enjoyyin.hive.proxy.jdbc.exception.IngoreEventException


/**
 * @author enjoyyin
 */
abstract class DaemonThread(override val threadName: String) extends LoopThread

trait LoopThread extends Logging {
  
  val threadName: String
  
  private val stopped = new AtomicBoolean(false)
  
  private var startTime = 0l
  
  private val eventThread: Thread = new Thread(threadName) {
    setDaemon(true)
    override def run(): Unit = {
      startTime = System.currentTimeMillis
      while (!stopped.get) {
        Utils.tryCatch(doLoop){
          case _: InterruptedException => return // exit even if eventQueue is not empty
          case NonFatal(e) => logError("Unexpected error in " + threadName, e)
        }
      }
    }
  }
  
  protected def doLoop: Unit
  
  def start: Unit = {
    if (stopped.get) {
      throw new IllegalStateException(threadName + " has already been stopped!")
    }
    // Call onStart before starting the event thread to make sure it happens before onReceive
    onStart
    eventThread.start()
  }
  
  def getStartTime: Long = startTime
  
  def stop: Unit = {
    if (stopped.compareAndSet(false, true)) {
      eventThread.interrupt()
      var onStopCalled = false
      Utils.tryCatch {
        eventThread.join()
        onStopCalled = true
        onStop
      } {
        case _: InterruptedException =>
          Thread.currentThread.interrupt()
          if (!onStopCalled) {
            onStop
          }
        case t => throw t
      }
    } else {
      // Keep quiet to allow calling `stop` multiple times.
    }
  }
  
  def isActive: Boolean = eventThread.isAlive
  
  protected def onStart: Unit = {}

  protected def onStop: Unit = {}

}

trait LoopCollection[E] extends Logging {
  
  def post(event: E): Unit
  
  val threadName: String
  
  protected def onReceiveSafety(event: E): Unit = Utils.tryCatch(onReceive(event)){
    case _: IngoreEventException => //ignore it
    case NonFatal(e) => Utils.tryCatch(onError(e)){
      case NonFatal(_) => logError("Unexpected error in " + threadName, e)
    }
  }
  
  protected def onReceive(event: E): Unit
  
  def remove(obj: E): Unit
  
  def remove(filter: E => Boolean): Unit
  
  protected def onError(e: Throwable): Unit
}