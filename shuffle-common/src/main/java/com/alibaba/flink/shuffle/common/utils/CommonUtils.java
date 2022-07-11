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

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.function.Supplier;

/** Utility methods can be used by all modules. */
public class CommonUtils {

    private static final char[] HEX_CHARS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    private static final int DEFAULT_RETRY_TIMES = 3;

    private static final ByteOrder DEFAULT_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    /**
     * Check the legality of method arguments. It will throw {@link IllegalArgumentException} if the
     * given condition is not true.
     */
    public static void checkArgument(boolean condition, @Nullable String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Check the legality of method arguments. It will throw {@link IllegalArgumentException} if the
     * given condition is not true.
     */
    public static void checkArgument(boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException("Illegal argument.");
        }
    }

    /**
     * Checks the legality of program state. It will throw {@link IllegalStateException} if the
     * given condition is not true.
     */
    public static void checkState(boolean condition, @Nullable String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    /**
     * Checks the legality of program state. It will throw {@link IllegalStateException} if the
     * given condition is not true.
     */
    public static void checkState(boolean condition, Supplier<String> message) {
        if (!condition) {
            throw new IllegalStateException(message.get());
        }
    }

    /**
     * Checks the legality of program state. It will throw {@link IllegalStateException} if the
     * given condition is not true.
     */
    public static void checkState(boolean condition) {
        if (!condition) {
            throw new IllegalStateException("Illegal state.");
        }
    }

    /** Generates a random byte array of the given length. */
    public static byte[] randomBytes(int length) {
        checkArgument(length > 0, "Must be positive.");

        Random random = new Random();
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    /** Converts the given byte array to a printable hex string. */
    public static String bytesToHexString(byte[] bytes) {
        checkArgument(bytes != null, "Must be not null.");

        char[] chars = new char[bytes.length * 2];

        for (int i = 0; i < chars.length; i += 2) {
            int index = i >>> 1;
            chars[i] = HEX_CHARS[(0xF0 & bytes[index]) >>> 4];
            chars[i + 1] = HEX_CHARS[0x0F & bytes[index]];
        }

        return new String(chars);
    }

    /** Allocates a piece of unmanaged direct {@link ByteBuffer} of the given size. */
    public static ByteBuffer allocateDirectByteBuffer(int size) {
        checkArgument(size >= 0, "Must be non-negative.");

        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        buffer.order(DEFAULT_BYTE_ORDER);
        return buffer;
    }
}
