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

import com.alibaba.flink.shuffle.core.listener.BacklogListener;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.storage.BufferWithBacklog;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReadingView;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/** A {@link DataPartitionReadingView} for test. */
public class FakedDataPartitionReadingView implements DataPartitionReadingView {

    private Queue<ByteBuf> buffersToSend = new ArrayDeque<>();
    private final DataListener dataListener;
    private final BacklogListener backlogListener;
    private final FailureListener failureListener;
    private Throwable cause;
    private boolean noMoreData;

    public FakedDataPartitionReadingView(
            DataListener dataListener,
            BacklogListener backlogListener,
            FailureListener failureListener) {
        this.dataListener = dataListener;
        this.backlogListener = backlogListener;
        this.failureListener = failureListener;
    }

    public synchronized void notifyBuffers(List<ByteBuf> buffers) {
        int numPrefResidual = buffersToSend.size();
        buffersToSend.addAll(buffers);
        if (numPrefResidual == 0) {
            dataListener.notifyDataAvailable();
        }
    }

    public synchronized void notifyBuffer(ByteBuf buffer) {
        if (buffersToSend.isEmpty()) {
            buffersToSend.add(buffer);
            dataListener.notifyDataAvailable();
        } else {
            buffersToSend.add(buffer);
        }
    }

    public void notifyBacklog(int backlog) {
        backlogListener.notifyBacklog(backlog);
    }

    @Override
    public synchronized BufferWithBacklog nextBuffer() {
        ByteBuf polled = buffersToSend.poll();
        if (polled == null) {
            return null;
        }
        return new BufferWithBacklog((Buffer) polled, buffersToSend.size());
    }

    @Override
    public synchronized void onError(Throwable throwable) {
        while (!buffersToSend.isEmpty()) {
            buffersToSend.poll().release();
        }
        cause = throwable;
    }

    public void setNoMoreData(boolean value) {
        noMoreData = value;
    }

    @Override
    public synchronized boolean isFinished() {
        return noMoreData && buffersToSend.isEmpty();
    }

    public Throwable getError() {
        return cause;
    }

    public void triggerFailure(Throwable t) {
        failureListener.notifyFailure(t);
    }

    public void setBuffersToSend(Queue<ByteBuf> buffersToSend) {
        this.buffersToSend = buffersToSend;
    }

    public DataListener getDataAvailableListener() {
        return dataListener;
    }
}
