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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** Utilities to handle {@link String} operations. */
public class StringUtils {

    public static final Charset UTF_8 = StandardCharsets.UTF_8;

    public static String bytesToString(byte[] inputBytes) {
        return new String(inputBytes, UTF_8);
    }

    public static byte[] stringToBytes(String inputString) {
        return inputString.getBytes(UTF_8);
    }
}
