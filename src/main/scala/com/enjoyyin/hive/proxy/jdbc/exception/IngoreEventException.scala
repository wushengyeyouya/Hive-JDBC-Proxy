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

package com.enjoyyin.hive.proxy.jdbc.exception

/**
 * @author enjoyyin
 */
class IngoreEventException extends Exception

class ValidateFailedException(msg: String, t: Throwable) extends Exception(msg, t) {
  def this(msg: String) = this(msg, null)
  def this(t: Throwable) = this(null, t)
}

class CanceledException(msg: String, t: Throwable) extends Exception(msg, t) {
  def this(msg: String) = this(msg, null)
  def this() = this(null, null)
  def this(t: Throwable) = this(null, t)
}