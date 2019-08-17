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

package com.enjoyyin.hive.proxy.jdbc.server

import java.net.InetSocketAddress

import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.auth.TSetIpAddressProcessor
import org.apache.thrift.TProcessor
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TServer
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.transport.TTransport
import com.enjoyyin.hive.proxy.jdbc.thrift.ThriftProxyService
import com.enjoyyin.hive.proxy.jdbc.util.Logging
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.maxWorkerThreads
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.minWorkerThreads
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.portNum
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.proxyHost
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.authTypeStr
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.AUTHENTICATION_OF_CUSTOM
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.AUTHENTICATION_OF_NONE
import com.enjoyyin.hive.proxy.jdbc.util.Utils
import com.enjoyyin.hive.proxy.jdbc.rule.basic.DefaultLoginValidateRule
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.conf.HiveConf

/**
 * @author enjoyyin
 */
class JDBCProxyServer extends Runnable with Logging {

  var server: TServer = _

  override def run(): Unit = {
    val serverAddress = getServerSocket(proxyHost, portNum)
    //hive.server2.authentication
    //hive.server2.custom.authentication.class
    authTypeStr.toUpperCase match {
      case AUTHENTICATION_OF_NONE =>
      case AUTHENTICATION_OF_CUSTOM =>
        val customClass = HiveConf.ConfVars.HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS
        System.setProperty(customClass.varname, DefaultLoginValidateRule.getClass.getName)
      case _ => throw new IllegalArgumentException(s"Illegal hive.server2.authentication of value $authTypeStr.")
    }
    logInfo("Is authentication enable? " + authTypeStr)
    val thriftProxyService = new ThriftProxyService
    thriftProxyService.init
    Utils.addShutdownHook(thriftProxyService.close)
    val sargs = new TThreadPoolServer.Args(serverAddress)
      .processorFactory(new SQLPlainProcessorFactory(thriftProxyService))
      .transportFactory(PlainSaslHelper.getPlainTransportFactory(authTypeStr))
      .protocolFactory(new TBinaryProtocol.Factory())
      .minWorkerThreads(minWorkerThreads)
      .maxWorkerThreads(maxWorkerThreads)

    server = new TThreadPoolServer(sargs)
    
    logInfo("JDBC Proxy listening on " + serverAddress.getServerSocket)

    server.serve()
  }

  def getServerSocket(proxyHost: String, portNum: Int): TServerSocket = {
    var serverAddress: InetSocketAddress = null
    if (StringUtils.isNotBlank(proxyHost)) {
      serverAddress = new InetSocketAddress(proxyHost, portNum)
    } else {
      serverAddress = new InetSocketAddress(portNum)
    }
    new TServerSocket(serverAddress)
  }

}

object JDBCProxyServer {
  def main(args: Array[String]): Unit = {
    val mainThread = new Thread(new JDBCProxyServer)
    mainThread.setName("hive-jdbc-proxy-main-thread")
    mainThread.start()
  }
}

class SQLPlainProcessorFactory(val service: ThriftProxyService)
    extends TProcessorFactory(null) {
  override def getProcessor(trans: TTransport): TProcessor = {
    new TSetIpAddressProcessor(service)
  }
}