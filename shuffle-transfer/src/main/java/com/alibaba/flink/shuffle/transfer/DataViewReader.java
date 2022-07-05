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

import com.alibaba.flink.shuffle.core.ids.ChannelID;
import com.alibaba.flink.shuffle.core.storage.BufferWithBacklog;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReadingView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/** A wrapper of {@link DataPartitionReadingView} providing credit related functionalities. */
public class DataViewReader {

    private static final Logger LOG = LoggerFactory.getLogger(ReadingService.class);

    private DataPartitionReadingView readingView;

    private int credit;

    private final ChannelID channelID;

    private final String addressStr;

    private final Consumer<DataViewReader> dataListener;

    public DataViewReader(
            ChannelID channelID, String addressStr, Consumer<DataViewReader> dataListener) {
        this.channelID = channelID;
        this.addressStr = addressStr;
        this.dataListener = dataListener;
    }

    public void setReadingView(DataPartitionReadingView readingView) {
        this.readingView = readingView;
    }

    public BufferWithBacklog getNextBuffer() throws Throwable {
        if (credit > 0) {
            BufferWithBacklog res = readingView.nextBuffer();
            if (res != null) {
                credit--;
            }
            return res;
        }
        return null;
    }

    public void addCredit(int credit) {
        this.credit += credit;
    }

    public int getCredit() {
        return credit;
    }

    public Consumer<DataViewReader> getDataListener() {
        return dataListener;
    }

    public boolean isEOF() {
        return readingView.isFinished();
    }

    public ChannelID getChannelID() {
        return channelID;
    }

    public DataPartitionReadingView getReadingView() {
        return readingView;
    }

    @Override
    public String toString() {
        return String.format("DataViewReader [channelID: %s, credit: %d]", channelID, credit);
    }
}
