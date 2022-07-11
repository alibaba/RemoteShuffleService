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

import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseChannel;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadData;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadHandshakeRequest;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * Reader client used to retrieve buffers from a remote shuffle worker to Flink TM. It talks with a
 * shuffle worker using Netty connection by language of {@link TransferMessage}. Flow control is
 * guaranteed by a credit based mechanism. The whole process of communication between {@link
 * ShuffleReadClient} and shuffle worker could be described as below:
 *
 * <ul>
 *   <li>1. Client opens connection and sends {@link ReadHandshakeRequest}, which contains number of
 *       initial credits -- indicates how many buffers it can accept;
 *   <li>2. Server sends {@link ReadData} by the view of the number of credits from client side.
 *       {@link ReadData} contains backlog information -- indicates how many more buffers to send;
 *   <li>3. Client allocates more buffers and sends {@link ReadAddCredit} to notify more credits;
 *   <li>4. Repeat from step-2 to step-3;
 *   <li>5. Client sends {@link CloseChannel} to server;
 * </ul>
 */
public abstract class ShuffleReadClient implements CreditListener {

    /** Create Netty connection to remote. */
    public abstract void connect() throws IOException, InterruptedException;

    /** Fire handshake. */
    public abstract void open() throws IOException;

    /** Called by Netty thread. */
    public abstract void dataReceived(ReadData readData);

    /** Called by Netty thread. */
    public abstract void backlogReceived(int backlog);

    /** Called by Netty thread. */
    public abstract void channelInactive();

    /** Called by Netty thread. */
    public abstract void exceptionCaught(Throwable t);

    /** Called by Netty thread to request buffer to receive data. */
    public abstract ByteBuf requestBuffer();

    /** Closes Netty connection -- called from task thread. */
    public abstract void close() throws IOException;
}
