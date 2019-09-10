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

import com.enjoyyin.hive.proxy.jdbc.util.ProxyConf._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import com.enjoyyin.hive.proxy.jdbc.exception.IngoreEventException
import java.util.concurrent.Executors
import com.enjoyyin.hive.proxy.jdbc.util.Utils
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.thrift.TBase
import java.util.concurrent.Callable
import scala.collection.JavaConversions._
import org.apache.hive.service.cli.thrift.TSessionHandle
import org.apache.hive.service.cli.thrift.TOperationHandle
import com.enjoyyin.hive.proxy.jdbc.util.EventLoop

/**
 * @author enjoyyin
 */
private[thrift] class ProxyServiceEventLoop(service: UserThriftProxyService) extends
  EventLoop[ProxyServiceEvent[_]]("proxy-service-event-loop-thread", MAX_CAPACITY_OF_USER_THRIFT_QUEUE) {
  private val runningFutures = new ConcurrentHashMap[TBase[_, _], (Future[Unit], Executor)]
  private var isStopped = false
  private val waitInterval = Math.min(MAX_WAIT_TIME_IN_QUEUE/5, 10000)
  private val cleaner = new Thread {
    setDaemon(true)
    override def run(): Unit = {
      while(!isStopped) {
    	  runningFutures.filter(kv => kv._2._2.startTime > 0 && System.currentTimeMillis - kv._2._2.startTime >= MAX_EXECUTE_TIME_OF_OPERATION)
    	   .foreach { case (tBase, (future, executor)) =>
    	    future.cancel(true)
          val msg = s"Execute time cannot more than ${Utils.msDurationToString(MAX_EXECUTE_TIME_OF_OPERATION)}."
    	    executor.event.putError(msg)
    	    logWarning(msg)
          executor.client.cancelOperation
    	    runningFutures -= tBase
    	  }
        Utils.tryIgnoreError(Thread.sleep(60000))
      }
    }
  }

    private[thrift] def removesInQueue(session: TSessionHandle): Unit = {
      remove { p =>
        val req = p.req
        val result = session == Utils.invokeAndIgnoreError(req, null, "getSessionHandle")
        if(result) {
          p.putError("Query has been canceled, for more information, please ask administrator for help!")
        }
        result
      }
      runningFutures.filter(_._1 == session).foreach { case (tBase, (future, executor)) =>
        future.cancel(true)
        executor.event.putError("Query has been canceled, for more information, please ask administrator for help!")
        logWarning("Query has been canceled, for more information, please ask administrator for help!")
        executor.client.cancelOperation
        runningFutures -= tBase
      }
    }

    private def get(event: ProxyServiceEvent[_]): ClientProxy = Utils.tryCatch(service.getClient(event)){t =>
      event.putError(t)
      throw t
    }

    private def getOrPut(event: ProxyServiceEvent[_]): ClientProxy = {
    	val startWaitTime = System.currentTimeMillis
      var client: ClientProxy = get(event)
      while(client == null &&
          System.currentTimeMillis - event.startWaitTime < MAX_WAIT_TIME_IN_QUEUE &&
          System.currentTimeMillis - startWaitTime < waitInterval) {
    	  Utils.tryIgnoreError(Thread.sleep(1000))
        client = get(event)
      }
      if (client == null) {
        if(System.currentTimeMillis - event.startWaitTime < MAX_WAIT_TIME_IN_QUEUE) {
          post(event)
        } else {
          event.putError(s"Cannot execute it since the query has waited for ${Utils.msDurationToString(MAX_WAIT_TIME_IN_QUEUE)} to execute.")
          logWarning(s"Cannot execute it since the query has waited for ${Utils.msDurationToString(MAX_WAIT_TIME_IN_QUEUE)} to execute, now remove it.")
        }
        throw new IngoreEventException
      }
      client
    }

    override def post(event: ProxyServiceEvent[_]) : Unit = {
      if(event.priority >= 4) {
        onReceive(event, getOrPut(event))
      } else {
        super.post(event)
      }
    }

    private def onReceive(event: ProxyServiceEvent[_], getClient:  => ClientProxy): Unit = {
      val req = event.req
      var session: TBase[_, _] = Utils.invokeAndIgnoreError(req, null, "getSessionHandle").asInstanceOf[TSessionHandle]
      if(session == null) {
        session = Utils.invokeAndIgnoreError(req, null, "getOperationHandle").asInstanceOf[TOperationHandle]
      }
      val executor = new Executor(event.asInstanceOf[ProxyServiceEvent[TBase[_, _]]], getClient, session)
      val future = ProxyServiceEventLoop.threadPool.submit(executor)
      runningFutures.put(session, (future, executor))
    }

    protected override def onReceive(event: ProxyServiceEvent[_]): Unit = {
      val client = getOrPut(event)
      onReceive(event, client)
    }

    private def defaultOnReceive(event: ProxyServiceEvent[_]): Unit = {
      event match {
        case getInfo: GetInfo =>
          val req = getInfo.req
          getInfo.putResult(getOrPut(event).GetInfo(req))
        case executeStatement: ExecuteStatement =>
          val req = executeStatement.req
          executeStatement.putResult(getOrPut(event).ExecuteStatement(req))
        case getTypeInfo: GetTypeInfo =>
          val req = getTypeInfo.req
          getTypeInfo.putResult(getOrPut(event).GetTypeInfo(req))
        case getCatalogs: GetCatalogs =>
          val req = getCatalogs.req
          getCatalogs.putResult(getOrPut(event).GetCatalogs(req))
        case getSchemas: GetSchemas =>
          val req = getSchemas.req
          getSchemas.putResult(getOrPut(event).GetSchemas(req))
        case getTables: GetTables =>
          val req = getTables.req
          getTables.putResult(getOrPut(event).GetTables(req))
        case getTableTypes: GetTableTypes =>
          val req = getTableTypes.req
          getTableTypes.putResult(getOrPut(event).GetTableTypes(req))
        case getColumns: GetColumns =>
          val req = getColumns.req
          getColumns.putResult(getOrPut(event).GetColumns(req))
        case getFunctions: GetFunctions =>
          val req = getFunctions.req
          getFunctions.putResult(getOrPut(event).GetFunctions(req))
        case getOperationStatus: GetOperationStatus =>
          val req = getOperationStatus.req
          getOperationStatus.putResult(getOrPut(event).GetOperationStatus(req))
        case cancelOperation: CancelOperation =>
          val req = cancelOperation.req
          cancelOperation.putResult(getOrPut(event).CancelOperation(req))
        case closeOperation: CloseOperation =>
          val req = closeOperation.req
          closeOperation.putResult(getOrPut(event).CloseOperation(req))
        case getResultSetMetadata: GetResultSetMetadata =>
          val req = getResultSetMetadata.req
          getResultSetMetadata.putResult(getOrPut(event).GetResultSetMetadata(req))
        case fetchResults: FetchResults =>
          val req = fetchResults.req
          fetchResults.putResult(getOrPut(event).FetchResults(req))
        case getDelegationToken: GetDelegationToken =>
          val req = getDelegationToken.req
          getDelegationToken.putResult(getOrPut(event).GetDelegationToken(req))
        case cancelDelegationToken: CancelDelegationToken =>
          val req = cancelDelegationToken.req
          cancelDelegationToken.putResult(getOrPut(event).CancelDelegationToken(req))
        case renewDelegationToken: RenewDelegationToken =>
          val req = renewDelegationToken.req
          renewDelegationToken.putResult(getOrPut(event).RenewDelegationToken(req))
      }
    }

    override def onError(e: Throwable): Unit = {
      logError("ProxyServiceEventLoop failed!", e)
    }

    override def onStop: Unit = {
      isStopped = true
      cleaner.interrupt()
    }

    override def onStart: Unit = {
      isStopped = false
      cleaner.start()
    }
    
    class Executor(val event: ProxyServiceEvent[TBase[_, _]], getClient: => ClientProxy, session: TBase[_, _]) extends Callable[Unit] {
      var startTime = 0l
      var client: ClientProxy = _
      override def call: Unit = {
        client = Utils.tryIgnoreError(getClient, null)
        if(client == null) return
        startTime = System.currentTimeMillis
        event.initStateBeforeExecute(startTime)
        val methodName = event.getClass.getSimpleName
        val result = Utils.invokeAndDealError(client, event.req, methodName, defaultOnReceive(event))
        if(result != null) {
          event.putResult(result.asInstanceOf[TBase[_, _]])
        }
        if(session != null) {
          runningFutures -= session
        }
      }
    }
}

private[thrift] object ProxyServiceEventLoop {
    private val threadFactory = new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("thrift-client-proxy-pool").build
    private[ProxyServiceEventLoop] val threadPool = Executors.newFixedThreadPool(MAX_EXECUTE_PROXY_CLIENT_POOL_SIZE, threadFactory)
    private[jdbc] def shutdown: Unit = threadPool.shutdown()
  }