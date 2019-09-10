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

import org.apache.hive.service.cli.HiveSQLException
import org.apache.hive.service.cli.thrift.TCLIService
import org.apache.hive.service.cli.thrift.TCancelDelegationTokenReq
import org.apache.hive.service.cli.thrift.TCancelDelegationTokenResp
import org.apache.hive.service.cli.thrift.TCancelOperationReq
import org.apache.hive.service.cli.thrift.TCancelOperationResp
import org.apache.hive.service.cli.thrift.TCloseOperationReq
import org.apache.hive.service.cli.thrift.TCloseOperationResp
import org.apache.hive.service.cli.thrift.TCloseSessionReq
import org.apache.hive.service.cli.thrift.TCloseSessionResp
import org.apache.hive.service.cli.thrift.TExecuteStatementReq
import org.apache.hive.service.cli.thrift.TExecuteStatementResp
import org.apache.hive.service.cli.thrift.TFetchResultsReq
import org.apache.hive.service.cli.thrift.TFetchResultsResp
import org.apache.hive.service.cli.thrift.TGetCatalogsReq
import org.apache.hive.service.cli.thrift.TGetCatalogsResp
import org.apache.hive.service.cli.thrift.TGetColumnsReq
import org.apache.hive.service.cli.thrift.TGetColumnsResp
import org.apache.hive.service.cli.thrift.TGetDelegationTokenReq
import org.apache.hive.service.cli.thrift.TGetDelegationTokenResp
import org.apache.hive.service.cli.thrift.TGetFunctionsReq
import org.apache.hive.service.cli.thrift.TGetFunctionsResp
import org.apache.hive.service.cli.thrift.TGetInfoReq
import org.apache.hive.service.cli.thrift.TGetInfoResp
import org.apache.hive.service.cli.thrift.TGetOperationStatusReq
import org.apache.hive.service.cli.thrift.TGetOperationStatusResp
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataReq
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataResp
import org.apache.hive.service.cli.thrift.TGetSchemasReq
import org.apache.hive.service.cli.thrift.TGetSchemasResp
import org.apache.hive.service.cli.thrift.TGetTableTypesReq
import org.apache.hive.service.cli.thrift.TGetTableTypesResp
import org.apache.hive.service.cli.thrift.TGetTablesReq
import org.apache.hive.service.cli.thrift.TGetTablesResp
import org.apache.hive.service.cli.thrift.TGetTypeInfoReq
import org.apache.hive.service.cli.thrift.TGetTypeInfoResp
import org.apache.hive.service.cli.thrift.TOpenSessionReq
import org.apache.hive.service.cli.thrift.TOpenSessionResp
import org.apache.hive.service.cli.thrift.TOperationHandle
import org.apache.hive.service.cli.thrift.TRenewDelegationTokenReq
import org.apache.hive.service.cli.thrift.TRenewDelegationTokenResp
import org.apache.hive.service.cli.thrift.TSessionHandle
import org.apache.hive.service.cli.thrift.TStatus
import org.apache.thrift.TBase
import com.enjoyyin.hive.proxy.jdbc.domain.HiveThriftServerInfo
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.DEFAULT_CLIENT_MAX_FREE_TIME
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.DEFAULT_MAX_INIT_FAILED_RECORDS
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.DEFAULT_QUERY_MAX_HOLD_TIME
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.DEFAULT_MAX_QUERY_FAILED_RECORDS
import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf.WAIT_FOR_CLOSE_MAX_TIME
import com.enjoyyin.hive.proxy.jdbc.util.Utils
import ClientState.ClientState
import ClientState.Free
import ClientState.GetResult
import ClientState.InUse
import ClientState.NotInit
import ClientState.Querying
import ClientState.WaitForClose
import javax.transaction.NotSupportedException
import java.util.concurrent.atomic.AtomicLong
import org.apache.hive.service.cli.thrift.TStatusCode


/**
 * @author enjoyyin
 */
private[thrift] class ClientProxy(thriftserverInfo: HiveThriftServerInfo, allocatedNums: AtomicLong)
  extends ClientInit(thriftserverInfo, allocatedNums) with TCLIService.Iface {
  
  private var state: ClientState = NotInit
  private var lastQueryTime, lastAcessTime: Long = 0l
  private var operationHandle: TOperationHandle = _
  private val lock = new java.util.concurrent.locks.ReentrantLock
//  private val failedRecords = new LinkedBlockingQueue[Long](DEFAULT_MAX_FAILED_RECORDS)
  private var continueQueryFailedNum = 0
  private var continueInitFailedNum = 0
  
  private def init: Unit = {
    if(state == NotInit) {
      lock.lock()
      if(state == NotInit) {
    	  Utils.tryCatchFinally{
          initClient
          continueInitFailedNum = 0
    		  state = Free
    		  operationHandle = null
          lastAcessTime = System.currentTimeMillis
    	  }{ t =>
          continueInitFailedNum += 1
          throw t
        }(lock.unlock())
      } else {
        lock.unlock()
      }
    }
  }
  
  
  def clear: Unit = {
    lock.lock()
    lastQueryTime = 0
    state = NotInit
    operationHandle = null
    continueQueryFailedNum = 0
    lastAcessTime = 0
    clearClient
    lock.unlock()
  }
  
  private[thrift] def getOperationHandle: Option[TOperationHandle] = {
    if((state == GetResult || state == WaitForClose) && operationHandle != null) {
      Some(operationHandle)
    } else {
      None
    }
  } 
  
  private[thrift] def isAvailable: Boolean = {
    if(isNotValidWithoutLock || state == InUse || state == Querying || state == GetResult) {
      return false
    }
    lock.lock()
    if(isNotValidWithoutLock) {
      lock.unlock()
      return false
    }
    if(state == NotInit) {
      Utils.tryThrow(init){t =>
        lock.unlock()
        t
      }
    }
    if(state == Free) {
      state = InUse
      lastAcessTime = System.currentTimeMillis
      lock.unlock()
      return true
    }
    lock.unlock()
    false
  }

  private[thrift] def isValid: Boolean = {
    if(!isNotValidWithoutLock) {
      return true
    }
    lock.lock()
    val isValid = !isNotValidWithoutLock
    lock.unlock()
    isValid
  }

  private def isNotValidWithoutLock: Boolean = continueInitFailedNum >= DEFAULT_MAX_INIT_FAILED_RECORDS

  private[thrift] def tryToValid : Boolean = {
    if(!isNotValidWithoutLock) {
      return true
    }
    lock.lock()
    if(!isNotValidWithoutLock) {
      lock.unlock()
      return true
    }
    clear
    val tmpInitFailedNum = continueInitFailedNum
    continueInitFailedNum = 0
    Utils.tryCatchFinally(isAvailable && testConnection){ t =>
      continueInitFailedNum = tmpInitFailedNum + 1
      false
    }{
      clear
      lock.unlock()
    }
  }

  private[thrift] def isFree: Boolean = {
    if(state != Free && state != NotInit) {
      return false
    }
    lock.lock()
    val isFree = state == Free || state == NotInit
    lock.unlock()
    isFree
  }

  private[thrift] def isAvailable(req: TBase[_, _]): Boolean = {
    if(isNotValidWithoutLock) return false
    val opHandle = Utils.invokeAndDealError(req, null, "getOperationHandle",
        t => errorOperation(t, {}, throw t)).asInstanceOf[TOperationHandle]
    if(opHandle != null) {
      if(state != GetResult && state != WaitForClose) {
        return false
      }
      lock.lock()
      if(isNotValidWithoutLock) {
        lock.unlock()
        return false
      }
      if((state == GetResult || state == WaitForClose) && operationHandle == opHandle) {
        lock.unlock()
        return true
      }
      lock.unlock()
      false
    } else {
      isAvailable
    }
  }
  
  private def canRelease: Boolean = {
    val waitForClose = state == WaitForClose && lastQueryTime > 0 &&
      System.currentTimeMillis - lastQueryTime >= WAIT_FOR_CLOSE_MAX_TIME
    val queryTimeout = state == GetResult && lastQueryTime > 0 &&
      System.currentTimeMillis - lastQueryTime >=DEFAULT_QUERY_MAX_HOLD_TIME
    waitForClose || queryTimeout
  }
  
  private def canFree: Boolean = {
    state == Free && lastAcessTime > 0 &&
      System.currentTimeMillis - lastAcessTime >= DEFAULT_CLIENT_MAX_FREE_TIME
  }
  
  private[thrift] def free: Unit = {
    if(canFree) {
      lock.lock()
      if(canFree) {
        logWarning(s"Because of reaching max free time, now clear thrift client $getClientName.")
        clear
      }
      lock.unlock()
    }
  }
  
  /**
   * 释放掉锁
   */
  private[thrift] def release: Unit = {
    if(canRelease) {
      lock.lock()
      if(canRelease) {
        logInfo(s"Because of reaching max hold time, since the state is $state, last query time is ${Utils.dateFormat(lastQueryTime)}, now release thrift client ${getClientName} for others.")
        lastQueryTime = 0
        state = Free
        operationHandle = null
      }
      lock.unlock()
    }
  }
  
  private def setSession[T](resp: T, session: TSessionHandle): Unit = {
    if(session != null) {
      Utils.invokeAndDealError(resp, session, "setSessionHandle", t => errorOperation(t, {}, throw t))
    }
  }
  
  private def errorOperation[T](t: Throwable, noSuchMethodDeal: => T, otherExceptionDeal: => T): T = {
    t match {
      case e: NoSuchMethodException => noSuchMethodDeal
      case e: Exception => otherExceptionDeal
    }
  }
  
  private def handler[T](req: Object, exception: TStatus => T): T = {
    lock.lock()
    if(state != InUse && state != GetResult && state != WaitForClose) {
      lock.unlock()
      return exception(HiveSQLException.toTStatus(new IllegalStateException(s"Proxy in wrong state: $state")))
    }
    state = Querying
    val tmpSession = Utils.invokeAndIgnoreError(req, null, "getSessionHandle").asInstanceOf[TSessionHandle]
    lastAcessTime = System.currentTimeMillis
    def noSuchMethod: Unit = if(req.isInstanceOf[TCloseOperationReq] || req.isInstanceOf[TCancelOperationReq]) {
        lastQueryTime = 0
        state = Free
        operationHandle = null
      } else {
        lastQueryTime = System.currentTimeMillis
        state = GetResult
      }
    Utils.tryCatchFinally {
      setSession(req, this.session)
      var methodName = req.getClass.getSimpleName
      methodName = methodName.substring(1, methodName.length - 3)
      val result: T = Utils.invoke(client, req, methodName).asInstanceOf[T]
      val opHandleValue = Utils.invokeAndDealError(result, null, "getOperationHandle",
          t => errorOperation(t, noSuchMethod, throw t))
      if(opHandleValue != null) {
        operationHandle = opHandleValue.asInstanceOf[TOperationHandle]
        if(!operationHandle.isHasResultSet) {
          state = WaitForClose
        } else {          
        	state = GetResult
        }
        lastQueryTime = System.currentTimeMillis
      } else if(operationHandle == null) {
        lastQueryTime = 0
        state = Free
      } else {
        lastQueryTime = System.currentTimeMillis
        val tStatus = Utils.invokeAndIgnoreError(result, null, "getStatus").asInstanceOf[TStatus]
        if(tStatus != null) {            
          tStatus.getStatusCode match {
            case TStatusCode.ERROR_STATUS | TStatusCode.INVALID_HANDLE_STATUS => state = WaitForClose
            case _ => 
          }
        }
      }
      setSession(result, tmpSession)
      continueQueryFailedNum = 0
      result
    } { e =>
        state = Free
        lastQueryTime = 0
        operationHandle = null
        handleException(e)
        exception(HiveSQLException.toTStatus(e.asInstanceOf[Exception]))
    } {
    	lastAcessTime = System.currentTimeMillis
    	Utils.tryIgnoreError(setSession(req, tmpSession))
    	lock.unlock()
    }
  }
  
  private[thrift] def cancelOperation = {
    if(operationHandle != null) {
      logWarning(s"Thrift client $getClientName ready to cancel a operation.")
      val req = new TCancelOperationReq
      req.setOperationHandle(operationHandle)
      Utils.tryAndLogError(client.CancelOperation(req))
    }
  }
  
  private def handleException(e: Throwable): Unit = {
    logWarning("", e)
    lock.lock()
    Utils.tryFinally {
      continueQueryFailedNum += 1
      if(continueQueryFailedNum >= DEFAULT_MAX_QUERY_FAILED_RECORDS) {
    		logWarning("Begin to reinit a new client, since it has reached max failed times in a continue query time.")
    		clear
    		Utils.tryAndLogError({init;logWarning("Reinit a new client succeed.")}, "Reinit a new Client failed, seems like thrift server has been wrong state.")
    	}
    } {
      lock.unlock()
    }
  }
  
  private def testConnection = {
    val req = new TExecuteStatementReq
    req.setSessionHandle(session)
    req.setStatement("select 1")
    val resp = client.ExecuteStatement(req)
    val status = resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS ||
      resp.getStatus.getStatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS
    if(status) {
      val cancel = new TCancelOperationReq
      cancel.setOperationHandle(resp.getOperationHandle)
      Utils.tryIgnoreError(client.CancelOperation(cancel))
    }
    status
  }
  
  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    throw new NotSupportedException("Not supported OpenSession.")
  }

  override def CloseSession(req: TCloseSessionReq): TCloseSessionResp = {
    throw new NotSupportedException("Not supported OpenSession.")
  }

  override def GetInfo(req: TGetInfoReq): TGetInfoResp = {
    handler(req, new TGetInfoResp(_, null))
  }

  override def ExecuteStatement(req: TExecuteStatementReq): TExecuteStatementResp = {
    logInfo(s"Proxy client $getClientName ready to execute hql: " + req.getStatement)
    handler(req, new TExecuteStatementResp(_))
  }

  override def GetTypeInfo(req: TGetTypeInfoReq): TGetTypeInfoResp = {
    handler(req, new TGetTypeInfoResp(_))
  }

  override def GetCatalogs(req: TGetCatalogsReq): TGetCatalogsResp = {
    handler(req, new TGetCatalogsResp(_))
  }

  override def GetSchemas(req: TGetSchemasReq): TGetSchemasResp = {
    handler(req, new TGetSchemasResp(_))
  }

  override def GetTables(req: TGetTablesReq): TGetTablesResp = {
    handler(req, new TGetTablesResp(_))
  }

  override def GetTableTypes(req: TGetTableTypesReq): TGetTableTypesResp = {
    handler(req, new TGetTableTypesResp(_))
  }

  override def GetColumns(req: TGetColumnsReq): TGetColumnsResp = {
    handler(req, new TGetColumnsResp(_))
  }

  override def GetFunctions(req: TGetFunctionsReq): TGetFunctionsResp = {
    handler(req, new TGetFunctionsResp(_))
  }

  override def GetOperationStatus(req: TGetOperationStatusReq): TGetOperationStatusResp = {
    handler(req, new TGetOperationStatusResp(_))
  }

  override def CancelOperation(req: TCancelOperationReq): TCancelOperationResp = {
    handler(req, new TCancelOperationResp(_))
  }

  override def CloseOperation(req: TCloseOperationReq): TCloseOperationResp = {
    handler(req, new TCloseOperationResp(_))
  }

  override def GetResultSetMetadata(req: TGetResultSetMetadataReq): TGetResultSetMetadataResp = {
    handler(req, new TGetResultSetMetadataResp(_))
  }

  override def FetchResults(req: TFetchResultsReq): TFetchResultsResp = {
    lock.lock()
    lastAcessTime = System.currentTimeMillis
    if(state != GetResult && state != WaitForClose) {
    	return new TFetchResultsResp(HiveSQLException.toTStatus(new IllegalStateException(s"Proxy in wrong state: $state")))
    }
    lastQueryTime = System.currentTimeMillis
    Utils.tryCatchFinally {
      val resp = client.FetchResults(req)
      if(resp.getResults == null || resp.getResults.getRows == null || resp.getResults.getRows.isEmpty) {
        state = WaitForClose
      }
      continueQueryFailedNum = 0
      resp
    } { case e: Exception =>
        handleException(e)
        new TFetchResultsResp(HiveSQLException.toTStatus(e))
    } {
      lastQueryTime = System.currentTimeMillis
      lastAcessTime = System.currentTimeMillis
      lock.unlock()
    }
  }

  override def GetDelegationToken(req: TGetDelegationTokenReq): TGetDelegationTokenResp = {
    handler(req, new TGetDelegationTokenResp(_))
  }

  override def CancelDelegationToken(req: TCancelDelegationTokenReq): TCancelDelegationTokenResp = {
    handler(req, new TCancelDelegationTokenResp(_))
  }

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    handler(req, new TRenewDelegationTokenResp(_))
  }
  
  override def toString: String = getClientName + "(state=" + state + ", lastAccessTime=" +
    Utils.dateFormat(lastAcessTime) + ", lastQueryTime=" + Utils.dateFormat(lastQueryTime) + ")"
  
}

object ClientState extends Enumeration {
  type ClientState = Value
  val NotInit, Free, InUse, Querying, GetResult, WaitForClose = Value
}