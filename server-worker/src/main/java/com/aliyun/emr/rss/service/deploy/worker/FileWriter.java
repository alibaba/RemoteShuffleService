/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.emr.rss.service.deploy.worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import scala.collection.mutable.ListBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.exception.AlreadyClosedException;
import com.aliyun.emr.rss.common.metrics.source.AbstractSource;
import com.aliyun.emr.rss.common.network.server.MemoryTracker;
import com.aliyun.emr.rss.common.protocol.PartitionSplitMode;

/*
 * Note: Once FlushNotifier.exception is set, the whole file is not available.
 *       That's fine some of the internal state(e.g. bytesFlushed) may be inaccurate.
 */
public final class FileWriter extends DeviceObserver {
  private static final Logger logger = LoggerFactory.getLogger(FileWriter.class);

  private static final long WAIT_INTERVAL_MS = 20;

  private final File file;
  private final FileChannel channel;
  public final File dataRootDir;
  private volatile boolean closed;

  private final AtomicInteger numPendingWrites = new AtomicInteger();
  private final ArrayList<Long> chunkOffsets = new ArrayList<>();
  private long nextBoundary;
  private long bytesFlushed;

  private final DiskFlusher flusher;
  private CompositeByteBuf flushBuffer;

  private final long chunkSize;
  private final long timeoutMs;

  private final long flushBufferSize;

  private final DeviceMonitor deviceMonitor;
  private final AbstractSource source; // metrics

  private long splitThreshold = 0;
  private final AtomicBoolean splitted = new AtomicBoolean(false);
  private final PartitionSplitMode splitMode;

  @Override
  public void notifyError(String deviceName, ListBuffer<File> dirs,
                          DeviceErrorType deviceErrorType) {
    if (!notifier.hasException()) {
      notifier.setException(new IOException("Device ERROR! Device: "
              + deviceName + " : " + deviceErrorType));
    }
    deviceMonitor.unregisterFileWriter(this);
  }

  static class FlushNotifier {
    final AtomicInteger numPendingFlushes = new AtomicInteger();
    final AtomicReference<IOException> exception = new AtomicReference<>();

    void setException(IOException e) {
      exception.set(e);
    }

    boolean hasException() {
      return exception.get() != null;
    }

    void checkException() throws IOException {
      IOException e = exception.get();
      if (e != null) {
        throw e;
      }
    }
  }

  private final FlushNotifier notifier = new FlushNotifier();

  public FileWriter(
      File file,
      DiskFlusher flusher,
      File workingDir,
      long chunkSize,
      long flushBufferSize,
      AbstractSource workerSource,
      RssConf rssConf,
      DeviceMonitor deviceMonitor,
      long splitThreshold,
      PartitionSplitMode splitMode) throws IOException {
    this.file = file;
    this.flusher = flusher;
    this.dataRootDir = workingDir;
    this.chunkSize = chunkSize;
    this.nextBoundary = chunkSize;
    this.chunkOffsets.add(0L);
    this.timeoutMs = RssConf.fileWriterTimeoutMs(rssConf);
    this.splitThreshold = splitThreshold;
    this.flushBufferSize = flushBufferSize;
    this.deviceMonitor = deviceMonitor;
    this.splitMode = splitMode;
    channel = new FileOutputStream(file).getChannel();
    source = workerSource;
    logger.debug("FileWriter {} split threshold {} mode {}", this, splitThreshold, splitMode);
    takeBuffer();
  }

  public File getFile() {
    return file;
  }

  public ArrayList<Long> getChunkOffsets() {
    return chunkOffsets;
  }

  public long getFileLength() {
    return bytesFlushed;
  }

  public void incrementPendingWrites() {
    numPendingWrites.incrementAndGet();
  }

  public void decrementPendingWrites() {
    numPendingWrites.decrementAndGet();
  }

  private void flush(boolean finalFlush) throws IOException {
    int numBytes = flushBuffer.readableBytes();
    notifier.checkException();
    notifier.numPendingFlushes.incrementAndGet();
    FlushTask task = new FlushTask(flushBuffer, channel, notifier);
    addTask(task);
    flushBuffer = null;
    bytesFlushed += numBytes;
    maybeSetChunkOffsets(finalFlush);
  }

  private void maybeSetChunkOffsets(boolean forceSet) {
    if (bytesFlushed >= nextBoundary || forceSet) {
      chunkOffsets.add(bytesFlushed);
      nextBoundary = bytesFlushed + chunkSize;
    }
  }

  private boolean isChunkOffsetValid() {
    // Consider a scenario where some bytes have been flushed
    // but the chunk offset boundary has not yet been updated.
    // we should check if the chunk offset boundary equals
    // bytesFlush or not. For example:
    // The last record is a giant record and it has been flushed
    // but its size is smaller than the nextBoundary, then the
    // chunk offset will not be set after flushing. we should
    // set it during FileWriter close.
    return chunkOffsets.get(chunkOffsets.size() - 1) == bytesFlushed;
  }

  /**
   * assume data size is less than chunk capacity
   *
   * @param data
   */
  public void write(ByteBuf data) throws IOException {
    if (closed) {
      String msg = "FileWriter has already closed!, fileName " + file.getAbsolutePath();
      logger.warn(msg);
      throw new AlreadyClosedException(msg);
    }

    if (notifier.hasException()) {
      return;
    }

    final int numBytes = data.readableBytes();
    MemoryTracker.instance().incrementDiskBuffer(numBytes);
    synchronized (this) {
      if (flushBuffer.readableBytes() != 0 &&
        flushBuffer.readableBytes() + numBytes >= this.flushBufferSize) {
        flush(false);
        takeBuffer();
      }

      data.retain();
      flushBuffer.addComponent(true, data);

      numPendingWrites.decrementAndGet();
    }
  }

  public long close() throws IOException {
    if (closed) {
      String msg = "FileWriter has already closed! fileName " + file.getAbsolutePath();
      logger.error(msg);
      throw new AlreadyClosedException(msg);
    }

    try {
      waitOnNoPending(numPendingWrites);
      closed = true;

      synchronized (this) {
        if (flushBuffer.readableBytes() > 0) {
          flush(true);
        }
        if (!isChunkOffsetValid()) {
          maybeSetChunkOffsets(true);
        }
      }

      waitOnNoPending(notifier.numPendingFlushes);
    } finally {
      returnBuffer();
      channel.close();

      // unregister from DeviceMonitor
      deviceMonitor.unregisterFileWriter(this);

    }

    return bytesFlushed;
  }

  public void destroy() {
    if (!closed) {
      closed = true;
      notifier.setException(new IOException("destroyed"));
      returnBuffer();
      try {
        channel.close();
      } catch (IOException e) {
        logger.warn("Close channel failed for file {} caused by {}.", file, e.getMessage());
      }
    }
    file.delete();

    if (splitted.get()) {
      String indexFileStr = file.getAbsolutePath() + PartitionFilesSorter.INDEX_SUFFIX;
      String sortedFileStr = file.getAbsolutePath() + PartitionFilesSorter.SORTED_SUFFIX;
      File indexFile = new File(indexFileStr);
      File sortedFile = new File(sortedFileStr);
      indexFile.delete();
      sortedFile.delete();
    }

    // unregister from DeviceMonitor
    deviceMonitor.unregisterFileWriter(this);
  }

  public IOException getException() {
    if (notifier.hasException()) {
      return notifier.exception.get();
    } else {
      return null;
    }
  }

  private void waitOnNoPending(AtomicInteger counter) throws IOException {
    long waitTime = timeoutMs;
    while (counter.get() > 0 && waitTime > 0) {
      try {
        notifier.checkException();
        TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MS);
      } catch (InterruptedException e) {
        IOException ioe = new IOException(e);
        notifier.setException(ioe);
        throw ioe;
      }
      waitTime -= WAIT_INTERVAL_MS;
    }
    if (counter.get() > 0) {
      IOException ioe = new IOException("Wait pending actions timeout.");
      notifier.setException(ioe);
      throw ioe;
    }
    notifier.checkException();
  }

  private void takeBuffer() {
    // metrics start
    String metricsName = null;
    String fileAbsPath = null;
    if (source.samplePerfCritical()) {
      metricsName = WorkerSource.TAKE_BUFFER_TIME();
      fileAbsPath = file.getAbsolutePath();
      source.startTimer(metricsName, fileAbsPath);
    }

    // real action
    flushBuffer = flusher.takeBuffer(timeoutMs);

    // metrics end
    if (source.samplePerfCritical()) {
      source.stopTimer(metricsName, fileAbsPath);
    }

    if (flushBuffer == null) {
      IOException e = new IOException("Take buffer timeout from DiskFlusher: "
        + flusher.bufferQueueInfo());
      notifier.setException(e);
    }
  }

  private void addTask(FlushTask task) throws IOException {
    if (!flusher.addTask(task, timeoutMs)) {
      IOException e = new IOException("Add flush task timeout.");
      notifier.setException(e);
      throw e;
    }
  }

  private synchronized void returnBuffer() {
    if (flushBuffer != null) {
      flusher.returnBuffer(flushBuffer);
      flushBuffer = null;
    }
  }

  public int hashCode() {
    return file.hashCode();
  }

  public boolean equals(Object obj) {
    return (obj instanceof FileWriter) &&
        file.equals(((FileWriter) obj).file);
  }

  public String toString() {
    return file.getAbsolutePath();
  }

  public void flushOnMemoryPressure() throws IOException {
    synchronized (this) {
      if (flushBuffer != null && flushBuffer.readableBytes() != 0) {
        flush(false);
        takeBuffer();
      }
    }
  }

  public void setSplitFlag() {
    splitted.set(true);
  }

  public long getSplitThreshold() {
    return splitThreshold;
  }

  public PartitionSplitMode getSplitMode() {
    return splitMode;
  }
}
