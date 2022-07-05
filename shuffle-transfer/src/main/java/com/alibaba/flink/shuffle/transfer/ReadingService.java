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
public class ReadingService {

    private static final Logger LOG = LoggerFactory.getLogger(ReadingService.class);

    private final PartitionedDataStore dataStore;

    private final Map<ChannelID, DataViewReader> servingChannels;

    private final Counter numReadingFlows;

    public ReadingService(PartitionedDataStore datastore) {
        this.dataStore = datastore;
        this.servingChannels = new HashMap<>();
        this.numReadingFlows = NetworkMetricsUtil.registerNumReadingFlows();
    }

    public void handshake(
            ChannelID channelID,
            DataSetID dataSetID,
            MapPartitionID mapID,
            int startSubIdx,
            int endSubIdx,
            Consumer<DataViewReader> dataListener,
            Consumer<Integer> backlogListener,
            Consumer<Throwable> failureHandler,
            int initialCredit,
            String addressStr)
            throws Throwable {

        checkState(
                !servingChannels.containsKey(channelID),
                () -> "Duplicate handshake for channel: " + channelID);
        DataViewReader dataViewReader = new DataViewReader(channelID, addressStr, dataListener);
        if (initialCredit > 0) {
            dataViewReader.addCredit(initialCredit);
        }
        long startTime = System.nanoTime();
        DataPartitionReadingView readingView =
                dataStore.createDataPartitionReadingView(
                        new ReadingViewContext(
                                dataSetID,
                                mapID,
                                startSubIdx,
                                endSubIdx,
                                () -> dataListener.accept(dataViewReader),
                                backlogListener::accept,
                                failureHandler::accept));
        LOG.debug(
                "(channel: {}) Reading handshake cost {} ms.",
                channelID,
                (System.nanoTime() - startTime) / 1000_000);
        dataViewReader.setReadingView(readingView);
        servingChannels.put(channelID, dataViewReader);
        numReadingFlows.inc();
    }

    public void addCredit(ChannelID channelID, int credit) {
        DataViewReader dataViewReader = servingChannels.get(channelID);
        if (dataViewReader == null) {
            return;
        }
        int oldCredit = dataViewReader.getCredit();
        dataViewReader.addCredit(credit);
        if (oldCredit == 0) {
            dataViewReader.getDataListener().accept(dataViewReader);
        }
    }

    public void readFinish(ChannelID channelID) {
        servingChannels.remove(channelID);
        numReadingFlows.dec();
    }

    public int getNumServingChannels() {
        return servingChannels.size();
    }

    public void closeAbnormallyIfUnderServing(ChannelID channelID) {
        DataViewReader dataViewReader = servingChannels.get(channelID);
        if (dataViewReader != null) {
            DataPartitionReadingView readingView = dataViewReader.getReadingView();
            readingView.onError(
                    new Exception(
                            String.format("(channel: %s) Channel closed abnormally", channelID)));
            servingChannels.remove(channelID);
            numReadingFlows.dec();
        }
    }

    public void releaseOnError(Throwable cause, ChannelID channelID) {
        if (channelID == null) {
            Set<ChannelID> channelIDs = servingChannels.keySet();
            LOG.error(
                    "Release channels -- {} on error.",
                    channelIDs.stream().map(ChannelID::toString).collect(Collectors.joining(", ")),
                    cause);
            for (DataViewReader dataViewReader : servingChannels.values()) {
                CommonUtils.runQuietly(() -> dataViewReader.getReadingView().onError(cause), true);
            }
            numReadingFlows.dec(getNumServingChannels());
            servingChannels.clear();
        } else if (servingChannels.containsKey(channelID)) {
            LOG.error("Release channel -- {} on error.", channelID, cause);
            CommonUtils.runQuietly(
                    () -> servingChannels.get(channelID).getReadingView().onError(cause), true);
            servingChannels.remove(channelID);
            numReadingFlows.dec();
        }
    }
}
