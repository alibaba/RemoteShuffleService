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

import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferSupplier;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;

import java.util.List;
import java.util.function.Consumer;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** A {@link DataPartitionWritingView} for test. */
public class FakedDataPartitionWritingView implements DataPartitionWritingView {

    private final List<Buffer> buffers;
    private volatile int regionStartCount;
    private volatile int regionFinishCount;
    private volatile boolean isFinished;
    private final Consumer<Buffer> dataHandler;
    private final DataRegionCreditListener dataRegionCreditListener;
    private final FailureListener failureListener;
    private Throwable cause;

    public FakedDataPartitionWritingView(
            DataSetID dataSetID,
            MapPartitionID mapPartitionID,
            Consumer<Buffer> dataHandler,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener,
            List<Buffer> buffers) {

        this.dataHandler = dataHandler;
        this.dataRegionCreditListener = dataRegionCreditListener;
        this.failureListener = failureListener;
        this.regionStartCount = 0;
        this.regionFinishCount = 0;
        this.isFinished = false;
        this.cause = null;
        this.buffers = buffers;
    }

    @Override
    public void onBuffer(Buffer buffer, ReducePartitionID reducePartitionID) {
        dataHandler.accept(buffer);
        buffer.clear();
        buffers.add(buffer);
        dataRegionCreditListener.notifyCredits(1, regionFinishCount);
    }

    @Override
    public void regionStarted(int dataRegionIndex, boolean isBroadcastRegion) {
        regionStartCount++;
        dataRegionCreditListener.notifyCredits(1, regionFinishCount);
    }

    @Override
    public void regionFinished() {
        regionFinishCount++;
    }

    @Override
    public void finish(DataCommitListener listener) {
        listener.notifyDataCommitted();
        release();
        isFinished = true;
    }

    private void release() {
        while (!buffers.isEmpty()) {
            buffers.remove(0).release();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        release();
        cause = throwable;
    }

    @Override
    public BufferSupplier getBufferSupplier() {
        return () -> {
            checkState(!buffers.isEmpty(), "No buffers available.");
            return buffers.remove(0);
        };
    }

    public List<Buffer> getCandidateBuffers() {
        return buffers;
    }

    public int getRegionStartCount() {
        return regionStartCount;
    }

    public int getRegionFinishCount() {
        return regionFinishCount;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void triggerFailure(Throwable t) {
        failureListener.notifyFailure(t);
    }

    public Throwable getError() {
        return cause;
    }
}
