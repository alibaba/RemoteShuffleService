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

package com.alibaba.flink.shuffle.core.storage;

import com.alibaba.flink.shuffle.core.ids.DataSetID;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * {@link DataSet} is a collection of {@link DataPartition}s which have the same {@link DataSetID}.
 * For example, {@link DataPartition}s produced by different parallel tasks of the same computation
 * vertex can have the same {@link DataSetID} and belong to the same {@link DataSet}.
 */
@NotThreadSafe
public class DataSet {

}
