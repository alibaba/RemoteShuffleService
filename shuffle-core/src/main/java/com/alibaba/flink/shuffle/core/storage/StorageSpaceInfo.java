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

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.util.Objects;

/** Storage space information for shuffle worker. */
@NotThreadSafe
public class StorageSpaceInfo implements Serializable {

    private static final long serialVersionUID = 8761204897411057994L;

    public static final StorageSpaceInfo ZERO_STORAGE_SPACE = new StorageSpaceInfo(0, 0, 0, 0);

    public static final StorageSpaceInfo INFINITE_STORAGE_SPACE =
            new StorageSpaceInfo(Long.MAX_VALUE, Long.MAX_VALUE, 0, 0);

    private volatile long ssdMaxFreeSpaceBytes;

    private volatile long hddMaxFreeSpaceBytes;

    private volatile long ssdMaxUsedSpaceBytes;

    private volatile long hddMaxUsedSpaceBytes;

    public StorageSpaceInfo(
            long hddMaxFreeSpaceBytes,
            long ssdMaxFreeSpaceBytes,
            long hddMaxUsedSpaceBytes,
            long ssdMaxUsedSpaceBytes) {
        this.hddMaxFreeSpaceBytes = hddMaxFreeSpaceBytes;
        this.ssdMaxFreeSpaceBytes = ssdMaxFreeSpaceBytes;
        this.hddMaxUsedSpaceBytes = hddMaxUsedSpaceBytes;
        this.ssdMaxUsedSpaceBytes = ssdMaxUsedSpaceBytes;
    }

    public void setHddMaxFreeSpaceBytes(long hddMaxFreeSpaceBytes) {
        this.hddMaxFreeSpaceBytes = hddMaxFreeSpaceBytes;
    }

    public void setSsdMaxFreeSpaceBytes(long ssdMaxFreeSpaceBytes) {
        this.ssdMaxFreeSpaceBytes = ssdMaxFreeSpaceBytes;
    }

    public void setHddMaxUsedSpaceBytes(long hddMaxUsedSpaceBytes) {
        this.hddMaxUsedSpaceBytes = hddMaxUsedSpaceBytes;
    }

    public void setSsdMaxUsedSpaceBytes(long ssdMaxUsedSpaceBytes) {
        this.ssdMaxUsedSpaceBytes = ssdMaxUsedSpaceBytes;
    }

    public long getHddMaxFreeSpaceBytes() {
        return hddMaxFreeSpaceBytes;
    }

    public long getSsdMaxFreeSpaceBytes() {
        return ssdMaxFreeSpaceBytes;
    }

    public long getHddMaxUsedSpaceBytes() {
        return hddMaxUsedSpaceBytes;
    }

    public long getSsdMaxUsedSpaceBytes() {
        return ssdMaxUsedSpaceBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StorageSpaceInfo that = (StorageSpaceInfo) o;
        return ssdMaxFreeSpaceBytes == that.ssdMaxFreeSpaceBytes
                && hddMaxFreeSpaceBytes == that.hddMaxFreeSpaceBytes
                && ssdMaxUsedSpaceBytes == that.ssdMaxUsedSpaceBytes
                && hddMaxUsedSpaceBytes == that.hddMaxUsedSpaceBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                ssdMaxFreeSpaceBytes,
                hddMaxFreeSpaceBytes,
                ssdMaxUsedSpaceBytes,
                hddMaxUsedSpaceBytes);
    }

    @Override
    public String toString() {
        return "StorageSpaceInfo{"
                + "ssdMaxFreeSpaceBytes="
                + ssdMaxFreeSpaceBytes
                + ", hddMaxFreeSpaceBytes="
                + hddMaxFreeSpaceBytes
                + ", ssdMaxUsedSpaceBytes="
                + ssdMaxUsedSpaceBytes
                + ", hddMaxUsedSpaceBytes="
                + hddMaxUsedSpaceBytes
                + '}';
    }
}
