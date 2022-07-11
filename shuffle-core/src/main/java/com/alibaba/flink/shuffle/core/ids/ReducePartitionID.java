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

package com.alibaba.flink.shuffle.core.ids;

import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.ReducePartition;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** ID of the {@link ReducePartition}. */
public class ReducePartitionID extends DataPartitionID {
    private final int partitionIndex;

    @Override
    public DataPartition.DataPartitionType getPartitionType() {
        return DataPartition.DataPartitionType.REDUCE_PARTITION;
    }

    public ReducePartitionID(int partitionIndex) {
        super(getBytes(partitionIndex));
        this.partitionIndex = partitionIndex;
    }

    private static byte[] getBytes(int value) {
        byte[] bytes = new byte[4];
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(value);
        return bytes;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }
}
