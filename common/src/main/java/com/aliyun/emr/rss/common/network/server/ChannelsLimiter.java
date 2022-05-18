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

package com.aliyun.emr.rss.common.network.server;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class ChannelsLimiter extends ChannelDuplexHandler
  implements MemoryTracker.MemoryTrackerListener {

  private static final Logger logger = LoggerFactory.getLogger(ChannelsLimiter.class);
  private final Set<Channel> channels = ConcurrentHashMap.newKeySet();
  private final String moduleName;

  public ChannelsLimiter(String moduleName) {
    this.moduleName = moduleName;
    MemoryTracker memoryTracker = MemoryTracker.instance();
    memoryTracker.registerMemoryListener(this);
  }

  private void pauseAllChannels() {
    channels.forEach(c -> {
      if (c.config().isAutoRead()) {
        c.config().setAutoRead(false);
      }
    });
  }

  private void trimCache() {
    channels.forEach(c->c.pipeline().fireUserEventTriggered(new TrimCache()));
  }

  private void resumeAllChannels() {
    channels.forEach(c -> {
      if (!c.config().isAutoRead()) {
        c.config().setAutoRead(true);
      }
    });
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    channels.add(ctx.channel());
    super.handlerAdded(ctx);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    if (!ctx.channel().config().isAutoRead()) {
      ctx.channel().config().setAutoRead(true);
    }
    channels.remove(ctx.channel());
    super.handlerRemoved(ctx);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
    if (evt instanceof TrimCache) {
      ((PooledByteBufAllocator) ctx.alloc()).trimCurrentThreadCache();
    }
  }

  @Override
  public void onPause(String moduleName) {
    if (this.moduleName.equals(moduleName)) {
      logger.info(this.moduleName + " channels pause read.");
      pauseAllChannels();
    }
  }

  @Override
  public void onResume(String moduleName) {
    if (moduleName.equalsIgnoreCase("all")) {
      logger.info(this.moduleName + " channels resume read.");
      resumeAllChannels();
    }
    if (this.moduleName.equals(moduleName)) {
      logger.info(this.moduleName + " channels resume read.");
      resumeAllChannels();
    }
  }

  @Override
  public void onTrim() {
    trimCache();
  }

  class TrimCache{
  }
}
