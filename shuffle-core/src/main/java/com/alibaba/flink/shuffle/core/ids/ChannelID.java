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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * A {@link BaseID} to identify a shuffle-read or shuffle-write transaction. Note that this is not
 * to identify a physical connection.
 */
public class ChannelID extends BaseID {

    private static final long serialVersionUID = -7984936977579866045L;

    public ChannelID() {
        super(16);
    }

    public ChannelID(byte[] id) {
        super(id);
    }

    public static ChannelID readFrom(ByteBuf byteBuf) {
        byte[] bytes = new byte[byteBuf.readInt()];
        byteBuf.readBytes(bytes);
        return new ChannelID(bytes);
    }

    @Override
    public String toString() {
        return String.format("ChannelID{ID=%s}", CommonUtils.bytesToHexString(id));
    }
}
