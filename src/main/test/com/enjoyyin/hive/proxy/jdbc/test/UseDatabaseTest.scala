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

package com.enjoyyin.hive.proxy.jdbc.test

import com.enjoyyin.hive.proxy.jdbc.thrift.ProxySession

object UseDatabaseTest extends App {

  testSQL("use abc")
  testSQL("use abc;")
  testSQL("use abc;select 1")
  testSQL("select 1 ; use abc ;")
  testSQL("select 1; use abc; select 1")

  private def testSQL(sql: String): Unit = {
    println("sql is " + sql)
    sql match {
      case ProxySession.USE_DATABASE_REGEX1(db) => println("exists " + db)
      case ProxySession.USE_DATABASE_REGEX2(db) => println("exists " + db)
      case ProxySession.USE_DATABASE_REGEX3(db) =>
        println("exists " + db)
      case ProxySession.USE_DATABASE_REGEX4(db) =>
        println("exists " + db)
      case _ => println("not exists.")
    }
  }

}
