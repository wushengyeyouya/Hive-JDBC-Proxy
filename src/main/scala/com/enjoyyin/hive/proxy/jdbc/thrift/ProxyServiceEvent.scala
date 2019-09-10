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

import org.apache.hive.service.cli.thrift.TGetInfoReq
import org.apache.hive.service.cli.thrift.TGetCatalogsReq
import org.apache.hive.service.cli.thrift.TRenewDelegationTokenReq
import org.apache.hive.service.cli.thrift.TExecuteStatementReq
import org.apache.hive.service.cli.thrift.TGetColumnsReq
import org.apache.hive.service.cli.thrift.TGetTablesReq
import org.apache.hive.service.cli.thrift.TGetOperationStatusReq
import org.apache.hive.service.cli.thrift.TFetchResultsReq
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataReq
import org.apache.hive.service.cli.thrift.TGetTypeInfoReq
import org.apache.hive.service.cli.thrift.TCancelOperationReq
import org.apache.hive.service.cli.thrift.TGetTableTypesReq
import org.apache.hive.service.cli.thrift.TGetFunctionsReq
import org.apache.hive.service.cli.thrift.TGetSchemasReq
import org.apache.hive.service.cli.thrift.TCancelDelegationTokenReq
import org.apache.hive.service.cli.thrift.TCloseOperationReq
import org.apache.hive.service.cli.thrift.TGetDelegationTokenReq
import org.apache.hive.service.cli.thrift.TGetInfoResp
import org.apache.hive.service.cli.thrift.TGetCatalogsResp
import org.apache.hive.service.cli.thrift.TFetchResultsResp
import org.apache.hive.service.cli.thrift.TGetSchemasResp
import org.apache.hive.service.cli.thrift.TCancelOperationResp
import org.apache.hive.service.cli.thrift.TGetTypeInfoResp
import org.apache.hive.service.cli.thrift.TGetDelegationTokenResp
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataResp
import org.apache.hive.service.cli.thrift.TExecuteStatementResp
import org.apache.hive.service.cli.thrift.TGetTableTypesResp
import org.apache.hive.service.cli.thrift.TGetOperationStatusResp
import org.apache.hive.service.cli.thrift.TCloseOperationResp
import org.apache.hive.service.cli.thrift.TRenewDelegationTokenResp
import org.apache.hive.service.cli.thrift.TGetFunctionsResp
import org.apache.hive.service.cli.thrift.TGetColumnsResp
import org.apache.hive.service.cli.thrift.TGetTablesResp
import org.apache.hive.service.cli.thrift.TCancelDelegationTokenResp
import org.apache.thrift.TBase
import com.enjoyyin.hive.proxy.jdbc.util.Utils
import com.enjoyyin.hive.proxy.jdbc.domain.User
import org.apache.hive.service.cli.thrift.TStatusCode
import org.apache.hive.service.cli.thrift.TStatus
import org.apache.commons.lang3.exception.ExceptionUtils

/**
 * @author enjoyyin
 */
object EventState extends Enumeration {
  type EventState = Value
  val Init, Running, OK, Failed = Value
}

import EventState._
case class EventInfo(startWaitTime: Long, startExecuteTime: Long, finishedExecuteTime: Long,
    state: EventState, hql: String, errorMsg: String, user: User)

private[jdbc] class ProxyServiceEvent[T](protected[thrift] val req: TBase[_, _], protected[thrift] val priority: Int,
    private[thrift] val startWaitTime: Long = System.currentTimeMillis) extends Comparable[ProxyServiceEvent[_]] {
  private var resp: Option[T] = None
	private var user: User = _
  private val lock = new Object
  private var startExecuteTime: Long = 0
  private var finishedExecuteTime: Long = 0
  private var state: EventState = Init
  private var errorMsg: String = _
  private[thrift] var specificPriority = 0
  
  override def compareTo(o: ProxyServiceEvent[_]): Int = {
    var value = o.priority - priority
    if(value == 0) {
      value = o.specificPriority - specificPriority
      if(value == 0) {
        (startWaitTime - o.startWaitTime).toInt
      } else {
        value
      }
    } else {
      value
    }
  }
  /**
   * 如果执行完成且是一个SQL查询，则返回一个EventInfo对象，否则返回None
   */
  def getEventInfo = if(resp.isDefined && req.isInstanceOf[TExecuteStatementReq]) {
    val hql = req.asInstanceOf[TExecuteStatementReq].getStatement
    if(startExecuteTime <= 0) {
   	  startExecuteTime = finishedExecuteTime
    }
    Some(EventInfo(startWaitTime, startExecuteTime, finishedExecuteTime, state, hql, errorMsg, user))
  } else None
  private[thrift] def initStateBeforeExecute(startExecuteTime: Long): Unit = {
    this.startExecuteTime = startExecuteTime
    state = Running
  }
  private[thrift] def setUser(user: User): Unit = this.user = user
  private[thrift] def getUser: User = user
  private[thrift] def isCompleted: Boolean = resp.isDefined
  private[thrift] def waitForComplete: T = {
    lock.synchronized {      
    	while(resp.isEmpty) {
    		lock.wait()
    	}
    }
    resp.get
  }
  private[thrift] def putResult(resp: T): Unit = {
    if(this.resp.isEmpty) {
      lock.synchronized {
        if(this.resp.isEmpty) {
          finishedExecuteTime = System.currentTimeMillis
          val tStatus = Utils.invokeAndDealError(resp, null, "getStatus", t => {
            errorMsg = ExceptionUtils.getStackTrace(t);state = Failed }).asInstanceOf[TStatus]
          if(tStatus != null) {            
        	  state = tStatus.getStatusCode match {
        	    case TStatusCode.ERROR_STATUS | TStatusCode.INVALID_HANDLE_STATUS => Failed
              case TStatusCode.STILL_EXECUTING_STATUS => Running
              case _ => OK
        	  }
            errorMsg = tStatus.getErrorMessage
          }
          this.resp = Some(resp)
          lock.notifyAll()
        }
      }
    }
  }
  private[thrift] def putError(msg: String): Unit = {
    putResult(getErrorResp(msg, null))
  }
  private[thrift] def putError(t: Throwable): Unit = {
    putResult(getErrorResp(null, t))
  }
  private def getErrorResp(msg: String, t: Throwable): T = {
    val className = "org.apache.hive.service.cli.thrift.T" + getClass.getSimpleName + "Resp"
    val resq = getClass.getClassLoader.loadClass(className).newInstance.asInstanceOf[T]
    val tStatus = new TStatus(TStatusCode.ERROR_STATUS)
    val errorMsg = new StringBuilder
    if(msg != null) {
      errorMsg ++= msg ++ " "
    }
    if(t != null) {
      errorMsg ++= ExceptionUtils.getStackTrace(t)
    }
    tStatus.setErrorMessage(errorMsg.toString)
    Utils.invokeAndDealError(resq, tStatus, "setStatus", {})
    resq
  }
}

case class GetInfo(override val req: TGetInfoReq, override val priority: Int = 1)
  extends ProxyServiceEvent[TGetInfoResp](req, priority)

case class ExecuteStatement(override val req: TExecuteStatementReq,
    override val priority: Int = 0) extends ProxyServiceEvent[TExecuteStatementResp](req, priority)

case class GetTypeInfo(override val req: TGetTypeInfoReq,
    override val priority: Int = 1) extends ProxyServiceEvent[TGetTypeInfoResp](req, priority)

case class GetCatalogs(override val req: TGetCatalogsReq,
    override val priority: Int = 1) extends ProxyServiceEvent[TGetCatalogsResp](req, priority)

case class GetSchemas(override val req: TGetSchemasReq,
    override val priority: Int = 2) extends ProxyServiceEvent[TGetSchemasResp](req, priority)

case class GetTables(override val req: TGetTablesReq,
    override val priority: Int = 3) extends ProxyServiceEvent[TGetTablesResp](req, priority)

case class GetTableTypes(override val req: TGetTableTypesReq,
    override val priority: Int = 3) extends ProxyServiceEvent[TGetTableTypesResp](req, priority)

case class GetColumns(override val req: TGetColumnsReq,
    override val priority: Int = 3) extends ProxyServiceEvent[TGetColumnsResp](req, priority)

case class GetFunctions(override val req: TGetFunctionsReq,
    override val priority: Int = 3) extends ProxyServiceEvent[TGetFunctionsResp](req, priority)

case class GetOperationStatus(override val req: TGetOperationStatusReq,
    override val priority: Int = 4) extends ProxyServiceEvent[TGetOperationStatusResp](req, priority)

case class CancelOperation(override val req: TCancelOperationReq,
    override val priority: Int = 6) extends ProxyServiceEvent[TCancelOperationResp](req, priority)

case class CloseOperation(override val req: TCloseOperationReq,
    override val priority: Int = 6) extends ProxyServiceEvent[TCloseOperationResp](req, priority)

case class GetResultSetMetadata(override val req: TGetResultSetMetadataReq,
    override val priority: Int = 4) extends ProxyServiceEvent[TGetResultSetMetadataResp](req, priority)

case class FetchResults(override val req: TFetchResultsReq,
    override val priority: Int = 5) extends ProxyServiceEvent[TFetchResultsResp](req, priority)

case class GetDelegationToken(override val req: TGetDelegationTokenReq,
    override val priority: Int = 2) extends ProxyServiceEvent[TGetDelegationTokenResp](req, priority)

case class CancelDelegationToken(override val req: TCancelDelegationTokenReq,
    override val priority: Int = 2) extends ProxyServiceEvent[TCancelDelegationTokenResp](req, priority)

case class RenewDelegationToken(override val req: TRenewDelegationTokenReq,
    override val priority: Int = 2) extends ProxyServiceEvent[TRenewDelegationTokenResp](req, priority)