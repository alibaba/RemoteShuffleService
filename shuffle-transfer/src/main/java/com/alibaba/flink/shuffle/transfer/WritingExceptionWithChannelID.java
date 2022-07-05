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

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.core.ids.ChannelID;

/** A {@link Throwable} with a related {@link ChannelID} while writing on shuffle worker. */
public class WritingExceptionWithChannelID extends ShuffleException {

    private static final long serialVersionUID = 1567641801077683004L;

    private final ChannelID channelID;

    public WritingExceptionWithChannelID(ChannelID channelID, Throwable t) {
        super(t);
        this.channelID = channelID;
    }

    public ChannelID getChannelID() {
        return channelID;
    }
}
