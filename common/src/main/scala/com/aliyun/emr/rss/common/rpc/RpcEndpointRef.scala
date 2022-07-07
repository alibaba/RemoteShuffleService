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
import scala.reflect.ClassTag

import com.aliyun.emr.rss.common.RssConf

abstract class RpcEndpointRef(conf: RssConf)
  extends Serializable {

  def address: RpcAddress

  def name: String

  def send(message: Any): Unit

  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  def askSync[T: ClassTag](message: Any): T = null.asInstanceOf[T]

  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    null.asInstanceOf[T]
  }
}
