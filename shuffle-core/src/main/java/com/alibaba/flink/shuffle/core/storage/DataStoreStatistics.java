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

import java.util.Objects;

/** Statistics information of {@link PartitionedDataStore}. */
public class DataStoreStatistics {

    public static final DataStoreStatistics EMPTY_DATA_STORE_STATISTICS =
            new DataStoreStatistics(0, 0, 0, 0, 0, 0);

    private final int numDataPartitions;

    private final long maxNumDataRegions;

    private final long totalIndexFileBytes;

    private final long totalDataFileBytes;

    private final long maxIndexFileBytes;

    private final long maxDataFileBytes;

    public DataStoreStatistics(
            int numDataPartitions,
            long maxNumDataRegions,
            long totalIndexFileBytes,
            long totalDataFileBytes,
            long maxIndexFileBytes,
            long maxDataFileBytes) {
        this.numDataPartitions = numDataPartitions;
        this.maxNumDataRegions = maxNumDataRegions;
        this.totalIndexFileBytes = totalIndexFileBytes;
        this.totalDataFileBytes = totalDataFileBytes;
        this.maxIndexFileBytes = maxIndexFileBytes;
        this.maxDataFileBytes = maxDataFileBytes;
    }

    public int getNumDataPartitions() {
        return numDataPartitions;
    }

    public long getMaxNumDataRegions() {
        return maxNumDataRegions;
    }

    public long getTotalIndexFileBytes() {
        return totalIndexFileBytes;
    }

    public long getTotalDataFileBytes() {
        return totalDataFileBytes;
    }

    public long getMaxIndexFileBytes() {
        return maxIndexFileBytes;
    }

    public long getMaxDataFileBytes() {
        return maxDataFileBytes;
    }

    public long getAvgIndexFileBytes() {
        return numDataPartitions == 0 ? 0 : totalIndexFileBytes / numDataPartitions;
    }

    public long getAvgDataFileBytes() {
        return numDataPartitions == 0 ? 0 : totalDataFileBytes / numDataPartitions;
    }

    public long getTotalPartitionFileBytes() {
        return totalIndexFileBytes + totalDataFileBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataStoreStatistics that = (DataStoreStatistics) o;
        return numDataPartitions == that.numDataPartitions
                && maxNumDataRegions == that.maxNumDataRegions
                && totalIndexFileBytes == that.totalIndexFileBytes
                && totalDataFileBytes == that.totalDataFileBytes
                && maxIndexFileBytes == that.maxIndexFileBytes
                && maxDataFileBytes == that.maxDataFileBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                numDataPartitions,
                maxNumDataRegions,
                totalIndexFileBytes,
                totalDataFileBytes,
                maxIndexFileBytes,
                maxDataFileBytes);
    }

    @Override
    public String toString() {
        return "DataStoreStatistics{"
                + "numDataPartitions="
                + numDataPartitions
                + ", maxNumDataRegions="
                + maxNumDataRegions
                + ", totalIndexFileBytes="
                + totalIndexFileBytes
                + ", totalDataFileBytes="
                + totalDataFileBytes
                + ", maxIndexFileBytes="
                + maxIndexFileBytes
                + ", maxDataFileBytes="
                + maxDataFileBytes
                + '}';
    }
}
