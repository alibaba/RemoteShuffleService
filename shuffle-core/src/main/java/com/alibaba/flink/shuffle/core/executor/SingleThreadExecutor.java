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

package com.alibaba.flink.shuffle.core.executor;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * A single-thread {@link Executor} implementation in order to avoid potential race condition and
 * simplify multi-threads logic design.
 */
public class SingleThreadExecutor implements Executor {

    /** Lock to protect shared structures and avoid potential race condition. */
    private final Object lock = new Object();

    /** The executor thread responsible for processing all the pending tasks. */
    private final ExecutorThread executorThread;

    /** All pending {@link Runnable} tasks waiting to be processed by this {@link Executor}. */
    @GuardedBy("lock")
    private final Queue<Runnable> tasks = new ArrayDeque<>();

    /** Whether this {@link Executor} has been shut down or not. */
    @GuardedBy("lock")
    private boolean isShutDown;

    public SingleThreadExecutor(String threadName) {
        CommonUtils.checkArgument(threadName != null, "Must be not null.");
        this.executorThread = new ExecutorThread(threadName);
        this.executorThread.start();
    }

    @Override
    public void execute(@Nonnull Runnable command) {
        synchronized (lock) {
            if (isShutDown) {
                throw new RejectedExecutionException("Executor has been shut down.");
            }

            try {
                boolean triggerProcessing = tasks.isEmpty();
                tasks.add(command);

                // notify the executor if there is no task available
                // for processing except for this newly added one
                if (triggerProcessing) {
                    lock.notify();
                }
            } catch (Throwable throwable) {
                throw new RejectedExecutionException("Failed to add new task.");
            }
        }
    }

    /** Returns true if the program is running in the main executor thread. */
    public boolean inExecutorThread() {
        return Thread.currentThread() == executorThread;
    }

    /**
     * Shuts down this {@link Executor} which releases all resources. After that, no task can be
     * processed any more.
     */
    public void shutDown() {
        synchronized (lock) {
            isShutDown = true;
            executorThread.interrupt();
        }
    }

    public boolean isShutDown() {
        synchronized (lock) {
            return isShutDown;
        }
    }

    /**
     * The executor thread which polls {@link Runnable} tasks from the task queue and executes the
     * polled tasks.
     */
    private class ExecutorThread extends Thread {

        private ExecutorThread(String threadName) {
            super(threadName);
        }

        @Override
        public void run() {
            do {
                List<Runnable> pendingTasks;
                synchronized (lock) {
                    // by design, only shut down or new tasks can wake up the
                    // executor thread, this while loop is added for safety
                    while (!isShutDown && tasks.isEmpty()) {
                        CommonUtils.runQuietly(lock::wait);
                    }

                    // exit only when this task executor has been shut down
                    if (isShutDown) {
                        tasks.clear();
                        break;
                    }

                    pendingTasks = new ArrayList<>(tasks);
                    tasks.clear();
                }

                // run all pending tasks one by one in FIFO order
                for (Runnable task : pendingTasks) {
                    CommonUtils.runQuietly(task::run);
                }
            } while (true);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // For test
    // ---------------------------------------------------------------------------------------------

    Thread getExecutorThread() {
        return executorThread;
    }
}
