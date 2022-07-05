/*
 * Copyright 2021 The Flink Remote Shuffle Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.transfer;

import com.alibaba.flink.shuffle.common.functions.BiConsumerWithException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

/** An implementation of {@link ChannelFutureListenerImpl} allows customizing processing logic. */
public class ChannelFutureListenerImpl implements ChannelFutureListener {

    private final BiConsumerWithException<ChannelFuture, Throwable, Exception> errorHandler;

    public ChannelFutureListenerImpl(
            BiConsumerWithException<ChannelFuture, Throwable, Exception> errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public void operationComplete(ChannelFuture channelFuture) throws Exception {
        if (!channelFuture.isSuccess()) {
            final Throwable cause;
            if (channelFuture.cause() != null) {
                cause = channelFuture.cause();
            } else {
                cause = new IllegalStateException("Sending cancelled.");
            }
            errorHandler.accept(channelFuture, cause);
        }
    }
}
