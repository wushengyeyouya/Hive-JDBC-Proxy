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

import org.apache.hive.service.cli.thrift.TOpenSessionResp
import org.apache.hive.service.cli.thrift.TOpenSessionReq
import org.apache.hive.service.cli.thrift.TCloseSessionReq
import org.apache.hive.service.cli.thrift.TCloseSessionResp
import org.apache.hive.service.cli.thrift.TGetInfoResp
import org.apache.hive.service.cli.thrift.TGetInfoReq
import org.apache.hive.service.cli.thrift.TExecuteStatementReq
import org.apache.hive.service.cli.thrift.TGetCatalogsReq
import org.apache.hive.service.cli.thrift.TGetTypeInfoReq
import org.apache.hive.service.cli.thrift.TGetTypeInfoResp
import org.apache.hive.service.cli.thrift.TGetTablesReq
import org.apache.hive.service.cli.thrift.TGetTablesResp
import org.apache.hive.service.cli.thrift.TGetTableTypesResp
import org.apache.hive.service.cli.thrift.TGetTableTypesReq
import org.apache.hive.service.cli.thrift.TGetColumnsResp
import org.apache.hive.service.cli.thrift.TGetColumnsReq
import org.apache.hive.service.cli.thrift.TGetFunctionsResp
import org.apache.hive.service.cli.thrift.TGetFunctionsReq
import org.apache.hive.service.cli.thrift.TGetOperationStatusReq
import org.apache.hive.service.cli.thrift.TGetOperationStatusResp
import org.apache.hive.service.cli.thrift.TCancelOperationReq
import org.apache.hive.service.cli.thrift.TCancelOperationResp
import org.apache.hive.service.cli.thrift.TCloseOperationReq
import org.apache.hive.service.cli.thrift.TCloseOperationResp
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataReq
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataResp
import org.apache.hive.service.cli.thrift.TFetchResultsResp
import org.apache.hive.service.cli.thrift.TFetchResultsReq
import org.apache.hive.service.cli.thrift.TGetDelegationTokenReq
import org.apache.hive.service.cli.thrift.TGetDelegationTokenResp
import org.apache.hive.service.cli.thrift.TCancelDelegationTokenReq
import org.apache.hive.service.cli.thrift.TCancelDelegationTokenResp
import org.apache.hive.service.cli.thrift.TRenewDelegationTokenResp
import org.apache.hive.service.cli.thrift.TRenewDelegationTokenReq
import org.apache.hive.service.cli.thrift.TGetSchemasReq
import org.apache.hive.service.cli.thrift.TGetSchemasResp
import org.apache.hive.service.cli.thrift.TGetCatalogsResp
import org.apache.hive.service.cli.thrift.TExecuteStatementResp
import org.apache.hive.service.cli.thrift.TStatus
import org.apache.hive.service.cli.thrift.TStatusCode
import org.apache.hive.service.cli.HiveSQLException
import org.apache.hive.service.auth.TSetIpAddressProcessor
import org.apache.hive.service.cli.thrift.TCLIService
import java.util.HashMap

import com.enjoyyin.hive.proxy.jdbc.util.Utils

/**
 * @author enjoyyin
 */
private[jdbc] class ThriftProxyService extends TCLIService.Iface with AbstractProxyService {
  
  private final val OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS)
  private final val ERROR_STATUS = new TStatus(TStatusCode.ERROR_STATUS)

  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    var user = TSetIpAddressProcessor.getUserName
    val ipAddress = TSetIpAddressProcessor.getUserIpAddress
    if (user == null) user = req.getUsername
    val resp = new TOpenSessionResp
    Utils.tryCatch {
      val sessionHandle = newSessionHandle
      resp.setSessionHandle(sessionHandle)
      validateSession(req.getConfiguration, user, ipAddress, sessionHandle)
      resp.setConfiguration(new HashMap[String, String])
      resp.setStatus(OK_STATUS)
    }{ case e: Exception =>
        logWarning(s"Error when opening a session for user($user, address: $ipAddress)", e)
        resp.setStatus(HiveSQLException.toTStatus(e));
    }
    resp
  }

  override def CloseSession(req: TCloseSessionReq): TCloseSessionResp = {
    val resp = new TCloseSessionResp
    Utils.tryCatch {
      closeSession(req.getSessionHandle)
      resp.setStatus(OK_STATUS)
    } { case e: Exception =>
        logWarning("Error closing session: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e));
    }
    resp
  }

  override def GetInfo(req: TGetInfoReq): TGetInfoResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.GetInfo(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

  override def ExecuteStatement(req: TExecuteStatementReq): TExecuteStatementResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.ExecuteStatement(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

  override def GetTypeInfo(req: TGetTypeInfoReq): TGetTypeInfoResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.GetTypeInfo(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

  override def GetCatalogs(req: TGetCatalogsReq): TGetCatalogsResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.GetCatalogs(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

  override def GetSchemas(req: TGetSchemasReq): TGetSchemasResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.GetSchemas(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

  override def GetTables(req: TGetTablesReq): TGetTablesResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.GetTables(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

  override def GetTableTypes(req: TGetTableTypesReq): TGetTableTypesResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.GetTableTypes(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

  override def GetColumns(req: TGetColumnsReq): TGetColumnsResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.GetColumns(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

  override def GetFunctions(req: TGetFunctionsReq): TGetFunctionsResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.GetFunctions(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

  override def GetOperationStatus(req: TGetOperationStatusReq): TGetOperationStatusResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.GetOperationStatus(req)
    putEvent(req.getOperationHandle, getInfo)
    getInfo.waitForComplete
  }

  override def CancelOperation(req: TCancelOperationReq): TCancelOperationResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.CancelOperation(req)
    putEvent(req.getOperationHandle, getInfo)
    getInfo.waitForComplete
  }

  override def CloseOperation(req: TCloseOperationReq): TCloseOperationResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.CloseOperation(req)
    putEvent(req.getOperationHandle, getInfo)
    getInfo.waitForComplete
  }

  override def GetResultSetMetadata(req: TGetResultSetMetadataReq): TGetResultSetMetadataResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.GetResultSetMetadata(req)
    putEvent(req.getOperationHandle, getInfo)
    getInfo.waitForComplete
  }

  override def FetchResults(req: TFetchResultsReq): TFetchResultsResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.FetchResults(req)
    putEvent(req.getOperationHandle, getInfo)
    getInfo.waitForComplete
  }

  override def GetDelegationToken(req: TGetDelegationTokenReq): TGetDelegationTokenResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.GetDelegationToken(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

  override def CancelDelegationToken(req: TCancelDelegationTokenReq): TCancelDelegationTokenResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.CancelDelegationToken(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    val getInfo = com.enjoyyin.hive.proxy.jdbc.thrift.RenewDelegationToken(req)
    putEvent(req.getSessionHandle, getInfo)
    getInfo.waitForComplete
  }

}