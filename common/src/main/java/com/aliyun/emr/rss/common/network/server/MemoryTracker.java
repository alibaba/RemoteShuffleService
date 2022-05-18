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

package com.aliyun.emr.rss.common.network.server;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.VM;

import com.aliyun.emr.rss.common.protocol.TransportModuleConstants;

public class MemoryTracker {
  private static final Logger logger = LoggerFactory.getLogger(MemoryTracker.class);
  private static volatile MemoryTracker _INSTANCE = null;
  private final long maxDirectorMemory = VM.maxDirectMemory();
  private final long pausePushDataThreshold;
  private final long pauseReplicateThreshold;
  private final long resumeThreshold;
  private final long maxSortMemory;
  private final List<MemoryTrackerListener> memoryTrackerListeners = new ArrayList<>();

  private final ScheduledExecutorService checkService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setNameFormat("MemoryTracker-check-thread").build());
  private final ScheduledExecutorService reportService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setNameFormat("MemoryTracker-report-thread").build());
  private final ExecutorService actionService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setNameFormat("MemoryTracker-action-thread").build());

  private AtomicLong nettyMemoryCounter = null;
  private final AtomicLong sortMemoryCounter = new AtomicLong(0);
  private final AtomicLong diskBufferCounter = new AtomicLong(0);
  private final LongAdder pausePushDataCounter = new LongAdder();
  private final LongAdder pausePushDataAndReplicateCounter = new LongAdder();
  private MemoryTrackerStat memoryTrackerStat = MemoryTrackerStat.resumeAll;
  private boolean underPressure;
  private int trimCount = 0;

  public static MemoryTracker initialize(
    double pausePushDataRatio,
    double pauseReplicateRatio,
    double resumeRatio,
    double maxSortRatio,
    int checkInterval,
    int reportInterval,
    int trimActionThreshold) {
    if (_INSTANCE == null) {
      _INSTANCE = new MemoryTracker(
        pausePushDataRatio,
        pauseReplicateRatio,
        resumeRatio,
        maxSortRatio,
        checkInterval,
        reportInterval,
        trimActionThreshold);
    }
    return _INSTANCE;
  }

  public void registerMemoryListener(MemoryTrackerListener listener) {
    synchronized (memoryTrackerListeners) {
      memoryTrackerListeners.add(listener);
    }
  }

  public static MemoryTracker instance() {
    return _INSTANCE;
  }

  private MemoryTracker(
    double pausePushDataRatio,
    double pauseReplicateRatio,
    double resumeRatio,
    double maxSortMemRatio,
    int checkInterval,
    int reportInterval,
    int trimActionThreshold) {
    maxSortMemory = ((long) (maxDirectorMemory * maxSortMemRatio));
    pausePushDataThreshold = (long) (maxDirectorMemory * pausePushDataRatio);
    pauseReplicateThreshold = (long) (maxDirectorMemory * pauseReplicateRatio);
    resumeThreshold = (long) (maxDirectorMemory * resumeRatio);

    initDirectMemoryIndicator();

    checkService.scheduleWithFixedDelay(() -> {
      try {
        MemoryTrackerStat lastAction = memoryTrackerStat;
        memoryTrackerStat = currentMemoryAction();
        if (lastAction != memoryTrackerStat) {
          if (memoryTrackerStat == MemoryTrackerStat.pausePushDataAndResumeReplicate) {
            pausePushDataCounter.increment();
            actionService.submit(() -> {
              logger.info("Trigger pausePushDataAndResumeReplicate action");
              memoryTrackerListeners.forEach(memoryTrackerListener ->
                memoryTrackerListener.onPause(TransportModuleConstants.PUSH_MODULE));
              memoryTrackerListeners.forEach(MemoryTrackerListener::onTrim);
              memoryTrackerListeners.forEach(memoryTrackerListener ->
                memoryTrackerListener.onResume(TransportModuleConstants.REPLICATE_MODULE));
            });
          } else if (memoryTrackerStat == MemoryTrackerStat.pausePushDataAndReplicate) {
            pausePushDataAndReplicateCounter.increment();
            actionService.submit(() -> {
              logger.info("Trigger pausePushDataAndReplicate action");
              memoryTrackerListeners.forEach(memoryTrackerListener ->
                memoryTrackerListener.onPause(TransportModuleConstants.PUSH_MODULE));
              memoryTrackerListeners.forEach(memoryTrackerListener ->
                memoryTrackerListener.onPause(TransportModuleConstants.REPLICATE_MODULE));
              memoryTrackerListeners.forEach(MemoryTrackerListener::onTrim);
            });
          } else {
            actionService.submit(() -> {
              logger.info("Trigger resume action");
              memoryTrackerListeners.forEach(memoryTrackerListener ->
                memoryTrackerListener.onResume("all"));
            });
          }
        } else {
          if (memoryTrackerStat != MemoryTrackerStat.resumeAll) {
            if (trimCount++ % trimActionThreshold == 0) {
              actionService.submit(() -> {
                logger.info("Trigger trim action");
                memoryTrackerListeners.forEach(MemoryTrackerListener::onTrim);
              });
              trimCount = 0;
            }
          }
        }
      } catch (Exception e) {
        logger.error("Memory tracker check error", e);
      }
    }, checkInterval, checkInterval, TimeUnit.MILLISECONDS);

    reportService.scheduleWithFixedDelay(() -> logger.info("Track all direct memory usage :{}/{}," +
        "disk buffer size:{}, sort memory size : {}",
        toMb(nettyMemoryCounter.get()), toMb(maxDirectorMemory),
        toMb(diskBufferCounter.get()), toMb(sortMemoryCounter.get())),
      reportInterval, reportInterval, TimeUnit.SECONDS);

    logger.info("Memory tracker initialized with :  " +
                  "\n max direct memory : {} ({} MB)" +
                  "\n pause pushdata memory : {} ({} MB)" +
                  "\n pause replication memory : {} ({} MB)" +
                  "\n resume memory : {} ({} MB)",
      maxDirectorMemory, toMb(maxDirectorMemory),
      pausePushDataThreshold, toMb(pausePushDataThreshold),
      pauseReplicateThreshold, toMb(pauseReplicateThreshold),
      resumeThreshold, toMb(resumeThreshold));
  }

  private double toMb(long bytes) {
    return bytes / 1024.0 / 1024.0;
  }

  private void initDirectMemoryIndicator() {
    try {
      Field field = null;
      Field[] result = PlatformDependent.class.getDeclaredFields();
      for (Field tf : result) {
        if ("DIRECT_MEMORY_COUNTER".equals(tf.getName())) {
          field = tf;
        }
      }
      field.setAccessible(true);
      nettyMemoryCounter = ((AtomicLong) field.get(PlatformDependent.class));
    } catch (Exception e) {
      logger.error("Fatal error, get netty_direct_memory failed, worker should stop", e);
      System.exit(-1);
    }
  }

  public MemoryTrackerStat currentMemoryAction() {
    long memoryUsage =  nettyMemoryCounter.get() + sortMemoryCounter.get();
    boolean pausePushData = memoryUsage > pausePushDataThreshold;
    boolean pauseReplication = memoryUsage > pauseReplicateThreshold;
    if (pausePushData) {
      underPressure = true;
      if (pauseReplication) {
        return MemoryTrackerStat.pausePushDataAndReplicate;
      } else {
        return MemoryTrackerStat.pausePushDataAndResumeReplicate;
      }
    } else {
      boolean resume = memoryUsage < resumeThreshold;
      if (resume) {
        underPressure = false;
        return MemoryTrackerStat.resumeAll;
      } else {
        if (underPressure) {
          return MemoryTrackerStat.pausePushDataAndResumeReplicate;
        } else {
          return MemoryTrackerStat.resumeAll;
        }
      }
    }
  }

  public interface MemoryTrackerListener {
    void onPause(String moduleName);

    void onResume(String moduleName);

    void onTrim();
  }

  public void reserveSortMemory(long fileLen) {
    sortMemoryCounter.addAndGet(fileLen);
  }

  public boolean sortMemoryReady() {
    return (currentMemoryAction().equals(MemoryTrackerStat.resumeAll)) &&
             sortMemoryCounter.get() < maxSortMemory;
  }

  public void releaseSortMemory(long size) {
    synchronized (this) {
      if (sortMemoryCounter.get() - size < 0) {
        sortMemoryCounter.set(0);
      } else {
        sortMemoryCounter.addAndGet(-1L * size);
      }
    }
  }

  public void incrementDiskBuffer(int size) {
    diskBufferCounter.addAndGet(size);
  }

  public void releaseDiskBuffer(int size) {
    diskBufferCounter.addAndGet(size * -1);
  }

  public AtomicLong getNettyMemoryCounter() {
    return nettyMemoryCounter;
  }

  public AtomicLong getSortMemoryCounter() {
    return sortMemoryCounter;
  }

  public AtomicLong getDiskBufferCounter() {
    return diskBufferCounter;
  }

  public long getPausePushDataCounter() {
    return pausePushDataCounter.sum();
  }

  public long getPausePushDataAndReplicateCounter() {
    return pausePushDataAndReplicateCounter.sum();
  }

  enum MemoryTrackerStat {
    resumeAll, pausePushDataAndReplicate, pausePushDataAndResumeReplicate
  }
}
