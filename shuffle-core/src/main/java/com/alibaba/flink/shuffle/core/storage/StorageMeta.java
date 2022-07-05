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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/** Meta information of the data storage for {@link DataPartition}. */
public class StorageMeta implements Serializable {

    private static final long serialVersionUID = 7636731224603174535L;

    protected final String storagePath;

    protected final StorageType storageType;

    /**
     * Same storage name means same underlying storage media, for example, different storage paths
     * (directory) may be on the same disk and have the same storage name.
     */
    private final String storageName;

    /** Remaining storage space in bytes on the target storage media. */
    protected volatile long numFreeSpaceBytes;

    protected volatile boolean isHealthy = true;

    /** Storage space in bytes already used on the target storage media by shuffle data. */
    protected volatile long numUsedSpaceBytes;

    public StorageMeta(String storagePath, StorageType storageType, String storageName) {
        CommonUtils.checkArgument(storagePath != null, "Must be not null.");
        CommonUtils.checkArgument(storageType != null, "Must be not null.");
        CommonUtils.checkArgument(storageName != null, "Must be not null.");

        this.storagePath = storagePath;
        this.storageType = storageType;
        this.storageName = storageName;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public String getStorageName() {
        return storageName;
    }

    public long getFreeStorageSpace() {
        return numFreeSpaceBytes;
    }

    public long getUsedStorageSpace() {
        return numUsedSpaceBytes;
    }

    public long updateFreeStorageSpace() {
        numFreeSpaceBytes = Long.MAX_VALUE;
        return numFreeSpaceBytes;
    }

    public void updateUsedStorageSpace(long numUsedSpaceBytes) {
        this.numUsedSpaceBytes = numUsedSpaceBytes;
    }

    public void updateStorageHealthStatus() {
        isHealthy = true;
    }

    public boolean isHealthy() {
        return isHealthy;
    }

    public void writeTo(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(storageType.name());
        dataOutput.writeUTF(storagePath);
    }

    public static StorageMeta readFrom(DataInput dataInput, DataPartitionFactory partitionFactory)
            throws IOException {
        StorageType storageType = StorageType.valueOf(dataInput.readUTF());
        String storagePath = dataInput.readUTF();
        return new StorageMeta(
                storagePath, storageType, partitionFactory.getStorageNameFromPath(storagePath));
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (that == null || getClass() != that.getClass()) {
            return false;
        }

        StorageMeta thatMeta = (StorageMeta) that;
        return Objects.equals(storagePath, thatMeta.storagePath)
                && storageType == thatMeta.storageType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(storagePath, storageType);
    }

    @Override
    public String toString() {
        return "StorageMeta{"
                + "StoragePath='"
                + storagePath
                + ", StorageType="
                + storageType
                + ", storageName="
                + storageName
                + '}';
    }
}
