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

package com.alibaba.flink.shuffle.common.utils;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests for utilities in {@link CommonUtils}. */
public class CommonUtilsTest {

    @Test
    public void testConcatAndSplitByteArrays() {
        byte[] first = new byte[] {1, 2, 3, 4};
        byte[] second = new byte[] {1, 2, 3};

        byte[] concat = CommonUtils.concatByteArrays(first, second);
        assertEquals(15, concat.length);

        List<byte[]> split = CommonUtils.splitByteArrays(concat);
        assertEquals(2, split.size());
        assertArrayEquals(first, split.get(0));
        assertArrayEquals(second, split.get(1));
    }

    @Test
    public void testConvertBetweenHexAndByteArray() {
        byte[] bytes = CommonUtils.randomBytes(16);
        String hex = CommonUtils.bytesToHexString(bytes);
        byte[] decoded = CommonUtils.hexStringToBytes(hex);
        assertArrayEquals(bytes, decoded);
    }
}
