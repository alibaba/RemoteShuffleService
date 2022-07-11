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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.ChannelID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReadingView;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.ReadingViewContext;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import com.alibaba.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * Harness used to read data from storage. It performs shuffle read by {@link PartitionedDataStore}.
 * The lifecycle is the same with a Netty {@link ChannelInboundHandler} instance.
 */
public interface ReadingService {

    public void handshake(
            ChannelID channelID,
            DataSetID dataSetID,
            MapPartitionID mapID,
            int startSubIdx,
            int endSubIdx,
            ReducePartitionID reduceID,
            int numSubs,
            Consumer<DataViewReader> dataListener,
            Consumer<Integer> backlogListener,
            Consumer<Throwable> failureHandler,
            int initialCredit,
            String addressStr);

    public void addCredit(ChannelID channelID, int credit);

    public void readFinish(ChannelID channelID);

    public void closeAbnormallyIfUnderServing(ChannelID channelID);
    public void releaseOnError(Throwable cause, ChannelID channelID);

}
