package com.enjoyyin.hive.proxy.jdbc.test

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.enjoyyin.hive.proxy.jdbc.util.Utils

/**
 * @author enjoyyin
 */
private object ThriftServerTest extends App {
  val sql = """show tables"""
  val test_url = "jdbc:hive2://localhost:10001/default"
  Class.forName("org.apache.hive.jdbc.HiveDriver")
  def test(index: Int) = {
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    Utils.tryFinally {
      conn = DriverManager.getConnection(test_url, "hduser0009", "")
      stmt = conn.createStatement
      rs = stmt.executeQuery(sql)
      while(rs.next) {
        println ("Date: " + Utils.dateFormat(System.currentTimeMillis) + ", " + index + ".tables => " + rs.getObject(1))
      }
      println("Date: " + Utils.dateFormat(System.currentTimeMillis) + ", ready to close " + index)
    } {
      if(rs != null) Utils.tryIgnoreError(rs.close())
      if(stmt != null) Utils.tryIgnoreError(stmt.close())
      if(conn != null) Utils.tryIgnoreError(conn.close())
    }
  }
  (0 until 8).foreach(i => new Thread {
    setName("thread-" + i)
    override def run(): Unit = {
      Utils.tryCatch(test(i)) { t =>
        println("Date: " + Utils.dateFormat(System.currentTimeMillis) + ", " + i + " has occur an error.")
        t.printStackTrace()
      }
    }
  }.start())
}
