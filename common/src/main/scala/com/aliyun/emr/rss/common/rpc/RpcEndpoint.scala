/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.emr.rss.common.rpc


private[rss] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

trait RpcEndpoint {
  val rpcEnv: RpcEnv

  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  def receive: PartialFunction[Any, Unit] = {
    case _ =>
  }

  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ =>
  }

  def checkRegistered(): Boolean = true

  def onError(cause: Throwable): Unit = {
    throw cause
  }

  def onConnected(remoteAddress: RpcAddress): Unit = {
  }

  def onDisconnected(remoteAddress: RpcAddress): Unit = {
  }

  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
  }

  def onStart(): Unit = {
  }

  def onStop(): Unit = {
  }

  final def stop(): Unit = {
  }
}