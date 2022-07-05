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

import com.alibaba.flink.shuffle.common.functions.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

/** Utility methods can be used by all modules. */
public class CommonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);

    private static final char[] HEX_CHARS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    private static final int DEFAULT_RETRY_TIMES = 3;

    private static final ByteOrder DEFAULT_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    /**
     * Ensures that the target object is not null and returns it. It will throw {@link
     * NullPointerException} if the target object is null.
     */
    public static <T> T checkNotNull(T object) {
        if (object == null) {
            throw new NullPointerException("Must be not null.");
        }
        return object;
    }

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

    /** Exists the current process and logs the error when any unrecoverable exception occurs. */
    public static void exitOnFatalError(Throwable throwable) {
        LOG.error("Exiting on fatal error.", throwable);
        FatalErrorExitUtils.exitProcessIfNeeded(-101);
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

    public static byte[] hexStringToBytes(String hexString) {
        byte[] bytes = new byte[hexString.length() / 2];
        for (int i = 0; i < hexString.length(); i += 2) {
            byte high = Byte.parseByte(hexString.charAt(i) + "", 16);
            byte low = Byte.parseByte(hexString.charAt(i + 1) + "", 16);
            bytes[i / 2] = (byte) ((high << 4) | low);
        }

        return bytes;
    }

    /** Generates a random hex string of the given length. */
    public static String randomHexString(int length) {
        checkArgument(length > 0, "Must be positive.");

        char[] chars = new char[length];
        Random random = new Random();

        for (int i = 0; i < length; ++i) {
            chars[i] = HEX_CHARS[random.nextInt(HEX_CHARS.length)];
        }

        return new String(chars);
    }

    public static byte[] concatByteArrays(byte[]... byteArrays) {
        int totalLength =
                Arrays.stream(byteArrays).map(array -> 4 + array.length).reduce(0, Integer::sum);
        ByteBuffer buffer = allocateHeapByteBuffer(totalLength);
        for (byte[] array : byteArrays) {
            buffer.putInt(array.length);
            buffer.put(array);
        }

        return buffer.array();
    }

    public static List<byte[]> splitByteArrays(byte[] concatArray) {
        List<byte[]> arrays = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(concatArray);
        while (buffer.hasRemaining()) {
            int length = buffer.getInt();
            byte[] array = new byte[length];
            buffer.get(array);
            arrays.add(array);
        }

        return arrays;
    }

    public static byte[] intToBytes(int i) {
        ByteBuffer bb = allocateHeapByteBuffer(4);
        bb.putInt(i);
        return bb.array();
    }

    public static byte[] longToBytes(long i) {
        ByteBuffer bb = allocateHeapByteBuffer(8);
        bb.putLong(i);
        return bb.array();
    }

    /**
     * Runs the given {@link RunnableWithException} in current thread silently and do nothing even
     * when any exception occurs.
     */
    public static void runQuietly(@Nullable RunnableWithException runnable) {
        runQuietly(runnable, false);
    }

    /**
     * Runs the given {@link RunnableWithException} in current thread and may log the encountered
     * exception if any.
     */
    public static void runQuietly(@Nullable RunnableWithException runnable, boolean logFailure) {
        if (runnable == null) {
            return;
        }

        try {
            runnable.run();
        } catch (Throwable throwable) {
            if (logFailure) {
                LOG.warn("Failed to run task.", throwable);
            }
        }
    }

    /**
     * Closes the target {@link AutoCloseable} and retries a maximum of {@link #DEFAULT_RETRY_TIMES}
     * times. It will throw exception if still fails after that.
     */
    public static void closeWithRetry(@Nullable AutoCloseable closeable) throws Exception {
        closeWithRetry(closeable, DEFAULT_RETRY_TIMES);
    }

    /**
     * Closes the target {@link AutoCloseable} and retries a maximum of the given times. It will
     * throw exception if still fails after that.
     */
    public static void closeWithRetry(@Nullable AutoCloseable closeable, int retryTimes)
            throws Exception {
        Throwable exception = null;
        for (int i = 0; i < retryTimes; ++i) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
                return;
            } catch (Throwable throwable) {
                exception = exception != null ? exception : throwable;
            }
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    /**
     * Deletes the target file and retries a maximum of {@link #DEFAULT_RETRY_TIMES} times. It will
     * throw exception if still fails after that.
     */
    public static void deleteFileWithRetry(@Nullable Path path) throws Exception {
        deleteFileWithRetry(path, DEFAULT_RETRY_TIMES);
    }

    /**
     * Deletes the target file and retries a maximum of the given times. It will throw exception if
     * still fails after that.
     */
    public static void deleteFileWithRetry(@Nullable Path path, int retryTimes) throws Exception {
        Throwable exception = null;
        for (int i = 0; i < retryTimes; ++i) {
            try {
                if (path != null) {
                    Files.deleteIfExists(path);
                }
                return;
            } catch (Throwable throwable) {
                exception = exception != null ? exception : throwable;
            }
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    /** Allocates a piece of unmanaged heap {@link ByteBuffer} of the given size. */
    public static ByteBuffer allocateHeapByteBuffer(int size) {
        checkArgument(size >= 0, "Must be non-negative.");

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.order(DEFAULT_BYTE_ORDER);
        return buffer;
    }

    /** Allocates a piece of unmanaged direct {@link ByteBuffer} of the given size. */
    public static ByteBuffer allocateDirectByteBuffer(int size) {
        checkArgument(size >= 0, "Must be non-negative.");

        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        buffer.order(DEFAULT_BYTE_ORDER);
        return buffer;
    }

    /** Casts the given long value to int and ensures there is no loss. */
    public static int checkedDownCast(long value) {
        int downCast = (int) value;
        if ((long) downCast != value) {
            throw new IllegalArgumentException(
                    "Cannot downcast long value " + value + " to integer.");
        }

        return downCast;
    }
}
