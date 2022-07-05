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

package com.alibaba.flink.shuffle.core.ids;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/** A abstract ID implementation based on bytes array. */
public abstract class BaseID implements Serializable {

    private static final long serialVersionUID = -2348171244792228610L;

    /** ID represented by a byte array. */
    protected final byte[] id;

    /** Pre-calculated hash-code for acceleration. */
    protected final int hashCode;

    public BaseID(byte[] id) {
        CommonUtils.checkArgument(id != null, "Must be not null.");

        this.id = id;
        this.hashCode = Arrays.hashCode(id);
    }

    public BaseID(int length) {
        CommonUtils.checkArgument(length > 0, "Must be positive.");

        this.id = CommonUtils.randomBytes(length);
        this.hashCode = Arrays.hashCode(id);
    }

    public byte[] getId() {
        return id;
    }

    /** Returns the number of bytes taken if this ID is serialized. */
    public int getFootprint() {
        return 4 + id.length;
    }

    /** Serializes this ID to the target {@link ByteBuf}. */
    public void writeTo(ByteBuf byteBuf) {
        byteBuf.writeInt(id.length);
        byteBuf.writeBytes(id);
    }

    /** Persists this ID to the target {@link DataOutput}. */
    public void writeTo(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id.length);
        dataOutput.write(id);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (that == null || getClass() != that.getClass()) {
            return false;
        }

        BaseID thatID = (BaseID) that;
        return hashCode == thatID.hashCode && Arrays.equals(id, thatID.id);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "BaseID{" + "ID=" + CommonUtils.bytesToHexString(id) + '}';
    }
}
