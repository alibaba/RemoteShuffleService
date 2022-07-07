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

import scala.concurrent.Future

import com.aliyun.emr.rss.common.RssConf

abstract class RpcEnv(conf: RssConf) {
  private[rss] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  def address: RpcAddress

  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    null
  }

  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    null
  }

  def stop(endpoint: RpcEndpointRef): Unit

  def shutdown(): Unit

  def awaitTermination(): Unit

  def deserialize[T](deserializationAction: () => T): T
}

private[rss] case class RpcEnvConfig(
                                      conf: RssConf,
                                      name: String,
                                      bindAddress: String,
                                      advertiseAddress: String,
                                      port: Int,
                                      numUsableCores: Int)
