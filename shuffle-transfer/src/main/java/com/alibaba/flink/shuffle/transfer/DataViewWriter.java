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
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;

/** A wrapper of {@link DataPartitionWritingView}. */
public class DataViewWriter {

    private final DataPartitionWritingView writingView;

    private final String addressStr;

    public DataViewWriter(DataPartitionWritingView writingView, String addressStr) {
        CommonUtils.checkArgument(writingView != null, "Must be not null.");
        CommonUtils.checkArgument(addressStr != null, "Must be not null.");

        this.writingView = writingView;
        this.addressStr = addressStr;
    }

    public DataPartitionWritingView getWritingView() {
        return writingView;
    }
}
