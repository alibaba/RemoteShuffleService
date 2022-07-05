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

package com.alibaba.flink.shuffle.transfer;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.functions.RunnableWithException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** A facility class for Netty test. */
public class AbstractNettyTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractNettyTest.class);

    protected InetSocketAddress address;

    protected JobID jobID;

    protected DataSetID dataSetID;

    protected ExecutorService executor;

    protected NettyConfig nettyConfig;

    protected TransferBufferPool transferBufferPool;

    private int prevAvailableBuffers;

    @Before
    public void setup() throws Exception {
        jobID = new JobID(CommonUtils.randomBytes(32));
        dataSetID = new DataSetID(CommonUtils.randomBytes(32));
        executor = Executors.newSingleThreadExecutor();
        nettyConfig = new NettyConfig(new Configuration());
        transferBufferPool = new TestTransferBufferPool(64, 64);
        prevAvailableBuffers = transferBufferPool.numBuffers();
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        assertEquals(prevAvailableBuffers, transferBufferPool.numBuffers());
        transferBufferPool.destroy();
    }

    protected void runAsync(RunnableWithException runnable) {
        executor.submit(
                () -> {
                    try {
                        runnable.run();
                    } catch (Throwable e) {
                        LOG.info("", e);
                    }
                });
    }

    protected ByteBuf requestBuffer() {
        return transferBufferPool.requestBuffer();
    }

    protected Queue<ByteBuf> constructBuffers(int numBuffers, int numLongsPerBuffer) {
        Queue<ByteBuf> res = new ArrayDeque<>();
        int number = 0;
        for (int i = 0; i < numBuffers; i++) {
            ByteBuf buffer = transferBufferPool.requestBuffer();
            for (int j = 0; j < numLongsPerBuffer; j++) {
                buffer.writeLong(number++);
            }
            res.add(buffer);
        }
        return res;
    }

    protected void verifyBuffers(int numBuffers, int numLongsPerBuffer, List<ByteBuf> buffers) {
        int a = 0;
        for (int i = 0; i < numBuffers; i++) {
            ByteBuf buffer = buffers.get(i);
            assertEquals(8 * numLongsPerBuffer, buffer.readableBytes());
            for (int j = 0; j < numLongsPerBuffer; j++) {
                assertEquals(a++, buffer.readLong());
            }
        }
    }

    protected void checkUntil(Runnable runnable) throws InterruptedException {
        Throwable lastThrowable = null;
        for (int i = 0; i < 100; i++) {
            try {
                runnable.run();
                return;
            } catch (Throwable t) {
                lastThrowable = t;
                Thread.sleep(200);
            }
        }
        LOG.info("", lastThrowable);
        fail();
    }

    protected void delayCheck(Runnable runnable) throws InterruptedException {
        Thread.sleep(100);
        runnable.run();
    }

    public static int getAvailablePort() {
        Random random = new Random();
        for (int i = 0; i < 1024; i++) {
            int port = random.nextInt(65535 - 10240) + 10240;
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                return serverSocket.getLocalPort();
            } catch (IOException ignored) {
            }
        }
        throw new RuntimeException("Could not find a free permitted port on the machine.");
    }
}
