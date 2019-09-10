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

import org.apache.hive.service.cli.thrift.TProtocolVersion

/**
 * @author enjoyyin
 */
private[thrift] object ClientConfiguration {
  val HIVE_AUTH_TYPE= "auth"
  val HIVE_AUTH_QOP = "sasl.qop"
  val HIVE_AUTH_SIMPLE = "noSasl"
  val HIVE_AUTH_TOKEN = "delegationToken"
  val HIVE_AUTH_USER = "user"
  val HIVE_AUTH_PRINCIPAL = "principal"
  val HIVE_AUTH_PASSWD = "password"
  val HIVE_AUTH_KERBEROS_AUTH_TYPE = "kerberosAuthType"
  val HIVE_AUTH_KERBEROS_AUTH_TYPE_FROM_SUBJECT = "fromSubject"
  val HIVE_ANONYMOUS_USER = "anonymous"
  val HIVE_ANONYMOUS_PASSWD = "anonymous"
  val HIVE_USE_SSL = "ssl"
  val HIVE_SSL_TRUST_STORE = "sslTrustStore"
  val HIVE_SSL_TRUST_STORE_PASSWORD = "trustStorePassword"
  val HIVE_SERVER2_TRANSPORT_MODE = "hive.server2.transport.mode"
  val HIVE_SERVER2_THRIFT_HTTP_PATH = "hive.server2.thrift.http.path"
  val supportedProtocols = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1 ::
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2 ::
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V3 ::
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V4 ::
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5 ::
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6 ::
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7 ::
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8 :: Nil
}