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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/** Utils when decoding. */
public class DecodingUtil {

    /**
     * Method to accumulate data from network.
     *
     * @param target {@link ByteBuf} to accumulate data to.
     * @param source {@link ByteBuf} from network.
     * @param targetAccumulationSize Target data size for the accumulation.
     * @param accumulatedSize Already accumulated size.
     * @return Whether accumulation is done.
     */
    public static boolean accumulate(
            ByteBuf target, ByteBuf source, int targetAccumulationSize, int accumulatedSize) {
        int copyLength = Math.min(source.readableBytes(), targetAccumulationSize - accumulatedSize);
        if (copyLength > 0) {
            target.writeBytes(source, copyLength);
        }
        return accumulatedSize + copyLength == targetAccumulationSize;
    }
}
