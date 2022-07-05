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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.MapPartition;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.DataInput;
import java.io.IOException;

/** ID of the {@link MapPartition}. */
public class MapPartitionID extends DataPartitionID {

    private static final long serialVersionUID = -7215025255975348400L;

    public MapPartitionID(byte[] resultPartitionID) {
        super(resultPartitionID);
    }

    @Override
    public DataPartition.DataPartitionType getPartitionType() {
        return DataPartition.DataPartitionType.MAP_PARTITION;
    }

    /** Deserializes and creates an {@link MapPartitionID} from the given {@link ByteBuf}. */
    public static MapPartitionID readFrom(ByteBuf byteBuf) {
        byte[] bytes = new byte[byteBuf.readInt()];
        byteBuf.readBytes(bytes);

        return new MapPartitionID(bytes);
    }

    /** Deserializes and creates an {@link MapPartitionID} from the given {@link DataInput}. */
    public static MapPartitionID readFrom(DataInput dataInput) throws IOException {
        byte[] bytes = new byte[dataInput.readInt()];
        dataInput.readFully(bytes);
        return new MapPartitionID(bytes);
    }

    @Override
    public String toString() {
        return "MapPartitionID{" + "ID=" + CommonUtils.bytesToHexString(id) + '}';
    }
}
