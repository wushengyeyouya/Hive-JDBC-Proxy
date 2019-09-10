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

import java.util.Locale
import java.util.Date
import org.apache.commons.lang3.time.DateFormatUtils

/**
 * @author enjoyyin
 */
object Utils extends Logging {
  
  def tryCatchFinally[T](tryOp: => T)(catchOp: Throwable => T)(finallyOp: => Unit): T = {
    try tryOp
    catch {
      case t: Throwable => catchOp(t)
    } finally finallyOp
  }

  def tryCatch[T](tryOp: => T)(catchOp: Throwable => T): T =
    try tryOp
    catch {
      case t: Throwable => catchOp(t)
    }

  def tryThrow[T](tryOp: => T)(catchOp: Throwable => Throwable): T = try tryOp
    catch {
      case t: Throwable => throw catchOp(t)
    }
  
  def tryIgnoreError[T](tryOp: => Unit): Unit = try tryOp
  catch {
    case _: Throwable => Unit
  }
  
  def tryIgnoreError[T](tryOp: => T, defaultValue: T): T = tryCatch(tryOp)(_ => defaultValue)
  
  def tryAndLogError[T](tryOp: => T, msg: String): T = tryCatch(tryOp) { t =>
    logWarning(msg, t)
    null.asInstanceOf[T]
  }
  
  def tryAndLogError[T](tryOp: => T): T = tryAndLogError(tryOp, "")
  
  def tryFinally[T](tryOp: => T)(finallyOp: => Unit): T = try tryOp finally  finallyOp
  
  def invoke(obj: Any, paramObjs: Any, methodName: String): Any = {
    if(paramObjs != null) {      
    	val method = obj.getClass.getMethod(methodName, paramObjs.getClass)
    	method.invoke(obj, paramObjs.asInstanceOf[Object])
    } else {
      val method = obj.getClass.getMethod(methodName)
      method.invoke(obj)
    }
  }
  
  def invokeAndIgnoreError(obj: Any, paramObjs: Any, methodName: String): Any = {
    invokeAndDealError(obj, paramObjs, methodName, {})
  }
  
  def invokeAndDealError(obj: Any, paramObjs: Any, methodName: String,
      errorOp: Throwable => Unit): Any = {
    try invoke(obj, paramObjs, methodName) catch {
      case t: Throwable => if(!t.isInstanceOf[NoSuchMethodException])logWarning("", t);errorOp(t);null
    }
  }
  
  def invokeAndDealError(obj: Any, paramObjs: Any, methodName: String,
      errorOp: => Unit): Any = {
    try invoke(obj, paramObjs, methodName) catch {
      case t: Throwable => if(!t.isInstanceOf[NoSuchMethodException])logWarning("", t);errorOp;null
    }
  }
  
  def addShutdownHook(op: => Unit): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run = {
        op
      }
    })
  }
  
  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
   */
  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  /**
   * Returns a human-readable string representing a duration such as "35ms"
   */
  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60 * second
    val hour = 60 * minute

    ms match {
      case t if t < second =>
        "%d ms".format(t)
      case t if t < minute =>
        "%.1f s".format(t.toFloat / second)
      case t if t < hour =>
        "%.1f m".format(t.toFloat / minute)
      case t =>
        "%.2f h".format(t.toFloat / hour)
    }
  }
  
  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to microseconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in ms.
   */
  def timeStringAsMs(str: String): Long = {
    ByteSizeUtils.timeStringAsMs(str)
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in seconds.
   */
  def timeStringAsSeconds(str: String): Long = {
    ByteSizeUtils.timeStringAsSec(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in bytes.
   */
  def byteStringAsBytes(str: String): Long = {
    ByteSizeUtils.byteStringAsBytes(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in kibibytes.
   */
  def byteStringAsKb(str: String): Long = {
    ByteSizeUtils.byteStringAsKb(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in mebibytes.
   */
  def byteStringAsMb(str: String): Long = {
    ByteSizeUtils.byteStringAsMb(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m, 500g) to gibibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in gibibytes.
   */
  def byteStringAsGb(str: String): Long = {
    ByteSizeUtils.byteStringAsGb(str)
  }

  /**
   * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of mebibytes.
   */
  def memoryStringToMb(str: String): Int = {
    // Convert to bytes, rather than directly to MB, because when no units are specified the unit
    // is assumed to be bytes
    (ByteSizeUtils.byteStringAsBytes(str) / 1024 / 1024).toInt
  }
  
  def dateFormat(date: Date, pattern: String) = DateFormatUtils.format(date, pattern)
  
  def dateFormat(date: Long, pattern: String) = DateFormatUtils.format(date, pattern)
  
  def dateFormat(date: Date) = DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss")
  
  def dateFormat(date: Long) = DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss")
  
}