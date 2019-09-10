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

import java.io.IOException
import scala.collection.JavaConversions.mutableMapAsJavaMap
import scala.collection.mutable.HashMap
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.auth.KerberosSaslHelper
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.auth.SaslQOP
import org.apache.hive.service.cli.HiveSQLException
import org.apache.hive.service.cli.thrift.TCLIService
import org.apache.hive.service.cli.thrift.TCloseSessionReq
import org.apache.hive.service.cli.thrift.TOpenSessionReq
import org.apache.hive.service.cli.thrift.TSessionHandle
import org.apache.thrift.TException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportException
import com.enjoyyin.hive.proxy.jdbc.domain.HiveThriftServerInfo
import com.enjoyyin.hive.proxy.jdbc.util.Logging
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf
import ClientConfiguration.HIVE_AUTH_KERBEROS_AUTH_TYPE
import ClientConfiguration.HIVE_AUTH_KERBEROS_AUTH_TYPE_FROM_SUBJECT
import ClientConfiguration.HIVE_AUTH_PRINCIPAL
import ClientConfiguration.HIVE_AUTH_QOP
import ClientConfiguration.HIVE_AUTH_SIMPLE
import ClientConfiguration.HIVE_AUTH_TOKEN
import ClientConfiguration.HIVE_AUTH_TYPE
import ClientConfiguration.HIVE_SSL_TRUST_STORE
import ClientConfiguration.HIVE_SSL_TRUST_STORE_PASSWORD
import ClientConfiguration.HIVE_USE_SSL
import ClientConfiguration.supportedProtocols
import javax.security.sasl.Sasl
import javax.security.sasl.SaslException
import com.enjoyyin.hive.proxy.jdbc.util.Utils
import java.util.concurrent.atomic.AtomicLong

/**
 * @author enjoyyin
 */
private[thrift] class ClientInit(thriftserverInfo: HiveThriftServerInfo, allocatedNums: AtomicLong) extends Logging {
  
  private var transport: TTransport = _
  protected var client: TCLIService.Iface = _
  protected var session: TSessionHandle = _
  private var clientName : String = _
  private val clientNum = allocatedNums.getAndIncrement
  
  def getClientName = clientName
  
  import thriftserverInfo._
  protected def initClient = {
      if(client == null) {
        val clientNameMsg = new StringBuilder
        clientNameMsg ++= thriftServerName ++ "-" ++ serverName ++ "(" ++ clientNum.toString + ")" ++
          "-" ++ (allocatedNums.getAndIncrement + 1 - thriftserverInfo.maxThread).toString
        clientName = clientNameMsg.toString
        logInfo(s"Init a new Thrift client $clientName.")
        Utils.tryCatch {
          transport = createBinaryTransport
          client = new TCLIService.Client(new TBinaryProtocol(transport))
          open
        } { e =>
          logError(s"Init a new Thrift client to $clientName failed.", e)
          clearClient
          throw e
        }
        logInfo(s"Init a new Thrift client $clientName succeed.")
    }
  }
  
  private def open = {
    val openReq = new TOpenSessionReq
    val openResp = client.OpenSession(openReq)
    org.apache.hive.jdbc.Utils.verifySuccess(openResp.getStatus)
      if (!supportedProtocols.contains(openResp.getServerProtocolVersion)) {
        throw new TException("Unsupported Hive2 protocol: " + openResp.getServerProtocolVersion)
      }
      session = openResp.getSessionHandle
  }
  
  protected def clearClient = {
    if(client != null) {
      logInfo(s"Clear a Thrift client $clientName.")
      val closeReq = new TCloseSessionReq(session)
      Utils.tryAndLogError(client.CloseSession(closeReq))
      client = null
      session = null
    }
    if (transport != null) {
      Utils.tryAndLogError(transport.close())
      transport = null
    }
  }
  
  private def createBinaryTransport: TTransport = {
    logInfo(s"Open transport to $host:$port...")
    var transport: TTransport = null
    Utils.tryCatch {
      // handle secure connection if specified
      if (!HIVE_AUTH_SIMPLE.equals(ProxyConf.get(HIVE_AUTH_TYPE))) {
        // If Kerberos
        val saslProps = HashMap[String, String]()
        var saslQOP = SaslQOP.AUTH
        if (ProxyConf.containsKey(HIVE_AUTH_PRINCIPAL)) {
          if (ProxyConf.containsKey(HIVE_AUTH_QOP)) {
            saslQOP = Utils.tryThrow(SaslQOP.fromString(ProxyConf.get(HIVE_AUTH_QOP))){
              case e : IllegalArgumentException =>
                new HiveSQLException("Invalid " + HIVE_AUTH_QOP + " parameter. " + e.getMessage, "42000", e)
            }
          }
          saslProps += Sasl.QOP -> saslQOP.toString
          saslProps += Sasl.SERVER_AUTH -> "true"
          val assumeSubject = HIVE_AUTH_KERBEROS_AUTH_TYPE_FROM_SUBJECT.equals(ProxyConf.get(HIVE_AUTH_KERBEROS_AUTH_TYPE))
          transport = KerberosSaslHelper.getKerberosTransport(
              ProxyConf.get(HIVE_AUTH_PRINCIPAL), host,
              HiveAuthFactory.getSocketTransport(host, port, loginTimeout), saslProps, assumeSubject)
        } else {
          val tokenStr = getClientDelegationTokenBefore1(ProxyConf.get(HIVE_AUTH_TYPE, null))
          if (tokenStr != null) {
            transport = KerberosSaslHelper.getTokenTransport(tokenStr,
                host, HiveAuthFactory.getSocketTransport(host, port, loginTimeout), saslProps)
          } else {
            if (isSslConnection) {
              val sslTrustStore = ProxyConf.get(HIVE_SSL_TRUST_STORE)
              val sslTrustStorePassword = ProxyConf.get(HIVE_SSL_TRUST_STORE_PASSWORD)
              if (sslTrustStore == null || sslTrustStore.isEmpty) {
                transport = HiveAuthFactory.getSSLSocket(host, port, loginTimeout)
              } else {
                transport = HiveAuthFactory.getSSLSocket(host, port, loginTimeout,
                    sslTrustStore, sslTrustStorePassword)
              }
            } else {
              transport = HiveAuthFactory.getSocketTransport(host, port, loginTimeout)
            }
            transport = PlainSaslHelper.getPlainTransport(username, password, transport)
          }
        }
      } else {
        // Raw socket connection (non-sasl)
        transport = HiveAuthFactory.getSocketTransport(host, port, loginTimeout)
      }
    } {
      case e: SaslException =>
        throw new HiveSQLException("Could not create secure connection to "
          + thriftServerName + ": " + e.getMessage, " 08S01", e)
      case e: TTransportException =>
        throw new HiveSQLException("Could not create connection to "
            + thriftServerName + ": " + e.getMessage, " 08S01", e)
    }
    if (!transport.isOpen) Utils.tryThrow(transport.open()) {
      case e: TTransportException =>
        new HiveSQLException("Could not open connection to " + thriftServerName + ": " + e.getMessage, " 08S01", e)
    }
    transport
  }
  
  private def getClientDelegationTokenBefore1(authType: String): String= {
    if (HIVE_AUTH_TOKEN.equalsIgnoreCase(authType)) Utils.tryCatch {
      val hadoopShims = ShimLoader.getHadoopShims
      val method = hadoopShims.getClass.getMethod("getTokenStrForm", classOf[String])
      method.invoke(hadoopShims, HiveAuthFactory.HS2_CLIENT_TOKEN).asInstanceOf[String]
    } {
      case e : IOException => throw new HiveSQLException("Error reading token ", e)
      case _: NoSuchMethodException => getClientDelegationTokenAfter1(authType)
    } else null
  }
  
  private def getClientDelegationTokenAfter1(authType: String): String = Utils.tryThrow {
    org.apache.hadoop.hive.shims.Utils.getTokenStrForm(HiveAuthFactory.HS2_CLIENT_TOKEN)
    } (new HiveSQLException("Error reading token ", _))
  
  private def isSslConnection: Boolean = {
    "true".equalsIgnoreCase(ProxyConf.get(HIVE_USE_SSL))
  }
}
