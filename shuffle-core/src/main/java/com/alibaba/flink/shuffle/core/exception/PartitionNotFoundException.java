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

package com.alibaba.flink.shuffle.core.exception;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;

/**
 * Exception to be thrown when the target partition can not be consumed which is a hint for data
 * producer to reproduce the corresponding data partition.
 */
public class PartitionNotFoundException extends ShuffleException {

    private static final long serialVersionUID = -4217817087530222073L;

    public PartitionNotFoundException(
            DataSetID dataSetID, DataPartitionID dataPartitionID, String message) {
        super(
                String.format(
                        "Data partition with %s and %s is not found: %s.",
                        dataSetID, dataPartitionID, message));
    }
}
