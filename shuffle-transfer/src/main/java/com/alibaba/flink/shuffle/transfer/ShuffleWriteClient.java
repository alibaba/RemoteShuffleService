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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.IOException;
/**
 * Writer client used to send buffers to a remote shuffle worker. It talks with a shuffle worker
 * using Netty connection by language of {@link TransferMessage}. Flow control is guaranteed by a
 * credit based mechanism. The whole process of communication between {@link ShuffleWriteClient} and
 * shuffle worker could be described as below:
 *
 * <ul>
 *   <li>1. Client opens connection and sends {@link TransferMessage.WriteHandshakeRequest};
 *   <li>2. Client sends {@link TransferMessage.WriteRegionStart}, which announces the start of a
 *       writing region and also indicates the number of buffers inside the region, we call it
 *       'backlog';
 *   <li>3. Server sends {@link TransferMessage.WriteAddCredit} to announce how many more buffers it
 *       can accept;
 *   <li>4. Client sends {@link TransferMessage.WriteData} based on server side 'credit';
 *   <li>5. Client sends {@link TransferMessage.WriteRegionFinish} to indicate writing finish of a
 *       region;
 *   <li>6. Repeat from step-2 to step-5;
 *   <li>7. Client sends {@link TransferMessage.WriteFinish} to indicate writing finish;
 *   <li>8. Server sends {@link TransferMessage.WriteFinishCommit} to confirm the writing finish.
 *   <li>9. Client sends {@link TransferMessage.CloseChannel} to server.
 * </ul>
 */
public abstract class ShuffleWriteClient {

    /** Initialize Netty connection and fire handshake. */
    public abstract void open() throws IOException, InterruptedException;

    /** Writes a piece of data to a subpartition. */
    public abstract void write(ByteBuf byteBuf, int subIdx) throws InterruptedException;

    /**
     * Indicates the start of a region. A region of buffers guarantees the records inside are
     * completed.
     */
    public abstract void regionStart(boolean isBroadcast, int numMaps, int requireCredit, boolean needMoreThanOneBuffer);

    /**
     * Indicates the finish of a region. A region is always bounded by a pair of region-start and
     * region-finish.
     */
    public abstract void regionFinish();

    /** Indicates the writing is finished. */
    public abstract void finish() throws InterruptedException;

    /** Closes Netty connection. */
    public abstract void close() throws IOException;

    /** Called by Netty thread. */
    public abstract void channelInactive();

    /** Called by Netty thread. */
    public abstract void exceptionCaught(Throwable t);

    public abstract void healthCheck();
}
