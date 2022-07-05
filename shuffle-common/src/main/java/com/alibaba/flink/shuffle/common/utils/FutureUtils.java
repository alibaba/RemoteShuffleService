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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A collection of utilities that expand the usage of {@link CompletableFuture}.
 *
 * <p>This class is partly copied from Apache Flink
 * (org.apache.flink.runtime.concurrent.FutureUtils).
 */
public class FutureUtils {

    /**
     * Run the given asynchronous action after the completion of the given future. The given future
     * can be completed normally or exceptionally. In case of an exceptional completion, the
     * asynchronous action's exception will be added to the initial exception.
     *
     * @param future to wait for its completion
     * @param composedAction asynchronous action which is triggered after the future's completion
     * @return Future which is completed after the asynchronous action has completed. This future
     *     can contain an exception if an error occurred in the given future or asynchronous action.
     */
    public static CompletableFuture<Void> composeAfterwards(
            CompletableFuture<?> future, Supplier<CompletableFuture<?>> composedAction) {
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        future.whenComplete(
                (Object outerIgnored, Throwable outerThrowable) -> {
                    final CompletableFuture<?> composedActionFuture = composedAction.get();

                    composedActionFuture.whenComplete(
                            (Object innerIgnored, Throwable innerThrowable) -> {
                                if (innerThrowable != null) {
                                    resultFuture.completeExceptionally(
                                            outerThrowable == null
                                                    ? innerThrowable
                                                    : outerThrowable);
                                } else if (outerThrowable != null) {
                                    resultFuture.completeExceptionally(outerThrowable);
                                } else {
                                    resultFuture.complete(null);
                                }
                            });
                });

        return resultFuture;
    }

    /**
     * Creates a {@link ConjunctFuture} which is only completed after all given futures have
     * completed. Unlike {@link FutureUtils#waitForAll(Collection)}, the resulting future won't be
     * completed directly if one of the given futures is completed exceptionally. Instead, all
     * occurring exception will be collected and combined to a single exception. If at least on
     * exception occurs, then the resulting future will be completed exceptionally.
     *
     * @param futuresToComplete futures to complete
     * @return Future which is completed after all given futures have been completed.
     */
    public static ConjunctFuture<Void> completeAll(
            Collection<? extends CompletableFuture<?>> futuresToComplete) {
        return new CompletionConjunctFuture(futuresToComplete);
    }

    /**
     * Returns an exceptionally completed {@link CompletableFuture}.
     *
     * @param cause to complete the future with
     * @param <T> type of the future
     * @return An exceptionally completed CompletableFuture
     */
    public static <T> CompletableFuture<T> completedExceptionally(Throwable cause) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(cause);
        return result;
    }

    /**
     * Creates a future that is complete once multiple other futures completed. The future fails
     * (completes exceptionally) once one of the futures in the conjunction fails. Upon successful
     * completion, the future returns the collection of the futures' results.
     *
     * <p>The ConjunctFuture gives access to how many Futures in the conjunction have already
     * completed successfully, via {@link ConjunctFuture#getNumFuturesCompleted()}.
     *
     * @param futures The futures that make up the conjunction. No null entries are allowed.
     * @return The ConjunctFuture that completes once all given futures are complete (or one fails).
     */
    public static <T> ConjunctFuture<Collection<T>> combineAll(
            Collection<? extends CompletableFuture<? extends T>> futures) {
        CommonUtils.checkArgument(futures != null, "Must be not null.");
        return new ResultConjunctFuture<>(futures);
    }

    /**
     * Creates a future that is complete once all of the given futures have completed. The future
     * fails (completes exceptionally) once one of the given futures fails.
     *
     * <p>The ConjunctFuture gives access to how many Futures have already completed successfully,
     * via {@link ConjunctFuture#getNumFuturesCompleted()}.
     *
     * @param futures The futures to wait on. No null entries are allowed.
     * @return The WaitingFuture that completes once all given futures are complete (or one fails).
     */
    public static ConjunctFuture<Void> waitForAll(
            Collection<? extends CompletableFuture<?>> futures) {
        CommonUtils.checkArgument(futures != null, "Must be not null.");
        return new WaitingConjunctFuture(futures);
    }

    /**
     * A future that is complete once multiple other futures completed. The futures are not
     * necessarily of the same type. The ConjunctFuture fails (completes exceptionally) once one of
     * the Futures in the conjunction fails.
     *
     * <p>The advantage of using the ConjunctFuture over chaining all the futures (such as via
     * {@link CompletableFuture#thenCombine(CompletionStage, BiFunction)} )}) is that ConjunctFuture
     * also tracks how many of the Futures are already complete.
     */
    public abstract static class ConjunctFuture<T> extends CompletableFuture<T> {

        /**
         * Gets the total number of Futures in the conjunction.
         *
         * @return The total number of Futures in the conjunction.
         */
        public abstract int getNumFuturesTotal();

        /**
         * Gets the number of Futures in the conjunction that are already complete.
         *
         * @return The number of Futures in the conjunction that are already complete
         */
        public abstract int getNumFuturesCompleted();
    }

    /**
     * Implementation of the {@link ConjunctFuture} interface which waits only for the completion of
     * its futures and does not return their values.
     */
    private static final class WaitingConjunctFuture extends ConjunctFuture<Void> {

        /** Number of completed futures. */
        private final AtomicInteger numCompleted = new AtomicInteger(0);

        /** Total number of futures to wait on. */
        private final int numTotal;

        /**
         * Method which increments the atomic completion counter and completes or fails the
         * WaitingFutureImpl.
         */
        private void handleCompletedFuture(Object ignored, Throwable throwable) {
            if (throwable == null) {
                if (numTotal == numCompleted.incrementAndGet()) {
                    complete(null);
                }
            } else {
                completeExceptionally(throwable);
            }
        }

        private WaitingConjunctFuture(Collection<? extends CompletableFuture<?>> futures) {
            this.numTotal = futures.size();

            if (futures.isEmpty()) {
                complete(null);
            } else {
                for (java.util.concurrent.CompletableFuture<?> future : futures) {
                    future.whenComplete(this::handleCompletedFuture);
                }
            }
        }

        @Override
        public int getNumFuturesTotal() {
            return numTotal;
        }

        @Override
        public int getNumFuturesCompleted() {
            return numCompleted.get();
        }
    }

    /**
     * The implementation of the {@link ConjunctFuture} which returns its Futures' result as a
     * collection.
     */
    private static class ResultConjunctFuture<T> extends ConjunctFuture<Collection<T>> {

        /** The total number of futures in the conjunction. */
        private final int numTotal;

        /** The number of futures in the conjunction that are already complete. */
        private final AtomicInteger numCompleted = new AtomicInteger(0);

        /** The set of collected results so far. */
        private final T[] results;

        /**
         * The function that is attached to all futures in the conjunction. Once a future is
         * complete, this function tracks the completion or fails the conjunct.
         */
        private void handleCompletedFuture(int index, T value, Throwable throwable) {
            if (throwable != null) {
                completeExceptionally(throwable);
            } else {
                /**
                 * This {@link #results} update itself is not synchronised in any way and it's fine
                 * because:
                 *
                 * <ul>
                 *   <li>There is a happens-before relationship for each thread (that is completing
                 *       the future) between setting {@link #results} and incrementing {@link
                 *       #numCompleted}.
                 *   <li>Each thread is updating uniquely different field of the {@link #results}
                 *       array.
                 *   <li>There is a happens-before relationship between all of the writing threads
                 *       and the last one thread (thanks to the {@code
                 *       numCompleted.incrementAndGet() == numTotal} check.
                 *   <li>The last thread will be completing the future, so it has transitively
                 *       happens-before relationship with all of preceding updated/writes to {@link
                 *       #results}.
                 *   <li>{@link AtomicInteger#incrementAndGet} is an equivalent of both volatile
                 *       read & write
                 * </ul>
                 */
                results[index] = value;

                if (numCompleted.incrementAndGet() == numTotal) {
                    complete(Arrays.asList(results));
                }
            }
        }

        @SuppressWarnings("unchecked")
        ResultConjunctFuture(Collection<? extends CompletableFuture<? extends T>> resultFutures) {
            this.numTotal = resultFutures.size();
            results = (T[]) new Object[numTotal];

            if (resultFutures.isEmpty()) {
                complete(Collections.emptyList());
            } else {
                int counter = 0;
                for (CompletableFuture<? extends T> future : resultFutures) {
                    final int index = counter;
                    counter++;
                    future.whenComplete(
                            (value, throwable) -> handleCompletedFuture(index, value, throwable));
                }
            }
        }

        @Override
        public int getNumFuturesTotal() {
            return numTotal;
        }

        @Override
        public int getNumFuturesCompleted() {
            return numCompleted.get();
        }
    }

    /**
     * {@link ConjunctFuture} implementation which is completed after all the given futures have
     * been completed. Exceptional completions of the input futures will be recorded but it won't
     * trigger the early completion of this future.
     */
    private static final class CompletionConjunctFuture extends ConjunctFuture<Void> {

        private final Object lock = new Object();

        private final int numFuturesTotal;

        private int futuresCompleted;

        private Throwable globalThrowable;

        private CompletionConjunctFuture(
                Collection<? extends CompletableFuture<?>> futuresToComplete) {
            numFuturesTotal = futuresToComplete.size();

            futuresCompleted = 0;

            globalThrowable = null;

            if (futuresToComplete.isEmpty()) {
                complete(null);
            } else {
                for (CompletableFuture<?> completableFuture : futuresToComplete) {
                    completableFuture.whenComplete(this::completeFuture);
                }
            }
        }

        private void completeFuture(Object ignored, Throwable throwable) {
            synchronized (lock) {
                futuresCompleted++;

                if (throwable != null) {
                    globalThrowable = globalThrowable == null ? throwable : globalThrowable;
                }

                if (futuresCompleted == numFuturesTotal) {
                    if (globalThrowable != null) {
                        completeExceptionally(globalThrowable);
                    } else {
                        complete(null);
                    }
                }
            }
        }

        @Override
        public int getNumFuturesTotal() {
            return numFuturesTotal;
        }

        @Override
        public int getNumFuturesCompleted() {
            synchronized (lock) {
                return futuresCompleted;
            }
        }
    }
}
