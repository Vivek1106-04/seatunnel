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

package org.apache.seatunnel.benchmark.checkpoint;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointSchedulingFixtureTest {

    private static final int PIPELINE_NUM = 4;
    private static final long CHECKPOINT_INTERVAL_MILLIS = 200L;
    private static final int SAMPLE_COUNT = 20;
    private static final long THREAD_STOP_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final long WARMUP_TRIGGERS = 500L;
    /**
     * Warm-up job ids start at 1,000,000, measured ones at 1,000, so only warm-up thread names
     * contain "/10000".
     */
    private static final String WARMUP_JOB_ID_PREFIX = "10000";

    @Test
    void shouldMeasureDueTriggersOfRealCoordinators() throws Exception {
        CheckpointSchedulingFixture fixture =
                new CheckpointSchedulingFixture(PIPELINE_NUM, CHECKPOINT_INTERVAL_MILLIS);
        fixture.setUp();
        try {
            assertTrue(
                    fixture.countSchedulerThreads() > 0,
                    "the coordinators should be running checkpoint scheduler threads");
            fixture.beginIteration();
            for (int i = 0; i < SAMPLE_COUNT; i++) {
                fixture.awaitNextDueTrigger();
                long due = System.nanoTime();
                long observed = fixture.awaitTrigger();
                assertTrue(
                        observed - due < TimeUnit.MILLISECONDS.toNanos(CHECKPOINT_INTERVAL_MILLIS),
                        "a due trigger should run well within one interval");
            }

            assertEquals(SAMPLE_COUNT, fixture.getSampled());
            fixture.endIteration();
        } finally {
            fixture.tearDown();
        }
    }

    @Test
    void shouldStopEverySchedulerThreadOnTearDown() throws Exception {
        // Stands in for a scheduler thread another test in this JVM has not finished stopping.
        CountDownLatch release = new CountDownLatch(1);
        Thread foreign = new Thread(() -> awaitQuietly(release), "checkpoint-foreign");
        foreign.setDaemon(true);
        foreign.start();
        try {
            CheckpointSchedulingFixture fixture =
                    new CheckpointSchedulingFixture(PIPELINE_NUM, CHECKPOINT_INTERVAL_MILLIS);
            fixture.setUp();
            fixture.tearDown();

            assertSchedulerThreadsStop(fixture);
            assertTrue(foreign.isAlive(), "the foreign thread should not have been waited for");
        } finally {
            release.countDown();
        }
    }

    @Test
    void shouldRemoveTheWarmUpCoordinatorsBeforeMeasuring() throws Exception {
        CheckpointSchedulingFixture fixture =
                new CheckpointSchedulingFixture(
                        PIPELINE_NUM, CHECKPOINT_INTERVAL_MILLIS, WARMUP_TRIGGERS);
        fixture.setUp();
        try {
            assertTrue(
                    fixture.getTriggeredCheckpoints() >= WARMUP_TRIGGERS,
                    "setup should return only after the warm-up triggers ran");
            // A per-pipeline scheduler names its threads after its job; none may be left from a
            // warm-up job once measuring starts.
            assertEquals(
                    Collections.emptyList(),
                    fixture.schedulerThreads().stream()
                            .map(Thread::getName)
                            .filter(name -> name.contains("/" + WARMUP_JOB_ID_PREFIX))
                            .collect(Collectors.toList()));
            fixture.beginIteration();
            for (int i = 0; i < SAMPLE_COUNT; i++) {
                fixture.awaitNextDueTrigger();
                fixture.awaitTrigger();
            }
            assertEquals(SAMPLE_COUNT, fixture.getSampled());
            fixture.endIteration();
        } finally {
            fixture.tearDown();
        }
        assertSchedulerThreadsStop(fixture);
    }

    private static void assertSchedulerThreadsStop(CheckpointSchedulingFixture fixture)
            throws InterruptedException {
        long deadline = System.nanoTime() + THREAD_STOP_TIMEOUT_NANOS;
        while (fixture.countSchedulerThreads() > 0 && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(CHECKPOINT_INTERVAL_MILLIS);
        }
        assertEquals(
                0L,
                fixture.countSchedulerThreads(),
                () ->
                        "still running: "
                                + fixture.schedulerThreads().stream()
                                        .map(Thread::getName)
                                        .collect(Collectors.toList()));
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void shouldRejectParametersOutsideTheSupportedRange() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointSchedulingFixture(0, 1_000L).setUp());
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointSchedulingFixture(1, 9L).setUp());
    }

    @Test
    void shouldSpaceMeasuredCoordinatorsAtLeastOneHundredMillisApart() {
        long interval = TimeUnit.MILLISECONDS.toNanos(CHECKPOINT_INTERVAL_MILLIS);

        assertEquals(1, CheckpointSchedulingFixture.probeStride(1, interval));
        assertEquals(2, CheckpointSchedulingFixture.probeStride(4, interval));
        assertEquals(5, CheckpointSchedulingFixture.probeStride(10, interval));
        assertEquals(5, CheckpointSchedulingFixture.probeStride(500, TimeUnit.SECONDS.toNanos(10)));
        assertEquals(
                3, CheckpointSchedulingFixture.probeStride(3, TimeUnit.MILLISECONDS.toNanos(10)));
    }

    @Test
    void shouldRejectTooManySkipsOnlyOnceEnoughTriggersWereDue() {
        // A one-second smoke iteration sees a handful of due triggers; one skip there is noise.
        assertFalse(CheckpointSchedulingFixture.isSkipShareTooHigh(7, 1));
        assertFalse(CheckpointSchedulingFixture.isSkipShareTooHigh(49, 49));

        assertFalse(CheckpointSchedulingFixture.isSkipShareTooHigh(100, 10));
        assertTrue(CheckpointSchedulingFixture.isSkipShareTooHigh(100, 11));
        assertTrue(CheckpointSchedulingFixture.isSkipShareTooHigh(50, 50));
    }

    @Test
    void shouldNameTheEngineFieldWhenItNoLongerExists() {
        IllegalStateException failure =
                assertThrows(
                        IllegalStateException.class,
                        () -> BenchmarkReflection.requireField(Object.class, "pendingCounter"));

        assertTrue(failure.getMessage().contains("java.lang.Object#pendingCounter"));
    }
}
