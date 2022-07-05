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

package com.alibaba.flink.shuffle.core.storage;

import javax.annotation.Nullable;

/**
 * When trying to read data from a {@link DataPartition}, the data consumer needs to call {@link
 * PartitionedDataStore#createDataPartitionReadingView} which will return an instance of this
 * interface, then the consumer can read data from the target {@link DataPartition} through the
 * {@link #nextBuffer} method.
 */
public interface DataPartitionReadingView {

    /** Reads a buffer from this reading view. Returns null if no buffer is available. */
    @Nullable
    BufferWithBacklog nextBuffer() throws Exception;

    /**
     * Notifies an error to the {@link DataPartitionReader} if the data consumer encounters any
     * unrecoverable failure.
     */
    void onError(Throwable throwable);

    /** Returns true if all target data has been consumed successfully. */
    boolean isFinished();
}
