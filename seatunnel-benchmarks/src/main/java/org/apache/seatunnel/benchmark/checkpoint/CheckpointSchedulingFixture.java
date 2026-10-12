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

import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * One real SeaTunnel member running {@code pipelineNum} real checkpoint coordinators, one per job,
 * whose periodic triggers the benchmark observes one at a time.
 *
 * <p>The coordinators are production code end to end; only the tasks are fakes, see {@link
 * FakeTaskCheckpointManager}. Whichever checkpoint scheduler the engine on the classpath uses is
 * the one being measured, so the same fixture runs unchanged on every revision being compared.
 *
 * <p>A trigger is observed through the coordinator's {@code pendingCounter}, which goes from 0 to 1
 * when the trigger body creates a pending checkpoint. The time a trigger is due is derived from the
 * previous observation plus one interval, since the coordinator re-arms itself with that delay. A
 * coordinator whose previous trigger was not observed has no known phase: it is taken out of the
 * rotation, counted as a skip, and resynchronised later.
 *
 * <p>Only every {@code probeStride}-th coordinator is measured; the rest are load. The probes start
 * at least {@link #MIN_PROBE_SPACING_NANOS} apart, which keeps two probes from coming due within
 * one sample of each other even after their phases drift. Measuring every coordinator instead would
 * skip whichever trigger came due while another was being measured, and those are the triggers that
 * bunch up under contention, so the skips would bias the result towards short delays exactly where
 * the scheduler is under the most pressure. Which coordinators are probes is fixed by index before
 * anything is measured, so probe samples carry no such selection, and probe triggers still collide
 * freely with the load.
 */
public final class CheckpointSchedulingFixture {

    /** Covers the per-pipeline pools and the member-wide scheduler threads alike. */
    static final String SCHEDULER_THREAD_NAME_PREFIX = "checkpoint-";

    /**
     * The coordinator has no getter for its count of in-flight checkpoints. It is read, never
     * written, to see when a trigger has created a pending checkpoint. The trigger body increments
     * it just before re-arming the next trigger, which is what makes "observed + interval" the next
     * due time. If the field is renamed or removed, loading this class fails naming it.
     */
    private static final Field PENDING_COUNTER_FIELD =
            BenchmarkReflection.requireField(CheckpointCoordinator.class, "pendingCounter");

    /** Lowest interval {@code CheckpointConfig} accepts. */
    static final long MIN_CHECKPOINT_INTERVAL_MILLIS = CheckpointConfig.MINIMAL_CHECKPOINT_TIME;

    /** Share of due triggers that may be skipped before an iteration is rejected. */
    static final double MAX_SKIP_RATIO = 0.1;

    /**
     * Fewest due triggers an iteration needs before {@link #MAX_SKIP_RATIO} is enforced. A short
     * smoke iteration, such as the CI benchmark job's one second, sees a handful of due triggers,
     * where a single skip is already above the ratio and says nothing about the run.
     */
    static final long MIN_DUE_TRIGGERS_FOR_SKIP_CHECK = 50;

    private static final int PIPELINE_ID = 1;
    private static final long FIRST_JOB_ID = 1_000L;
    private static final long NOT_SYNCED = Long.MIN_VALUE;

    /**
     * Bounds of how long before a due trigger the setup stops parking and starts spinning. Parking
     * alone would add the OS timer slack to the start of the measured window, and parking wakes
     * late by that slack: tens of microseconds on Linux, about 5 ms on macOS. The window starts at
     * the minimum and widens to the largest overshoot seen plus the minimum, up to the maximum.
     */
    private static final long MIN_SPIN_WINDOW_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    private static final long MAX_SPIN_WINDOW_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    /**
     * Resync waits at most this many intervals, plus {@link #PENDING_REARM_NANOS}, for every
     * coordinator to trigger once.
     */
    private static final int RESYNC_INTERVALS = 3;

    /**
     * A trigger that finds a checkpoint still pending re-arms itself after 500 ms instead of one
     * interval; resync allows for one such re-arm, with margin.
     */
    private static final long PENDING_REARM_NANOS = TimeUnit.SECONDS.toNanos(1);

    /** Least spacing between the start phases of two measured coordinators. */
    private static final long MIN_PROBE_SPACING_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

    private static final long EXECUTOR_KEEP_ALIVE_SECONDS = 60L;
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30L;

    /**
     * Shape of the throwaway coordinators that warm up the trigger path: 100 jobs at a 200 ms
     * interval trigger about 500 checkpoints a second, ten times the rate of any measured point.
     * Their job ids are far above the measured ones.
     */
    private static final int WARMUP_PIPELINES = 100;

    private static final long WARMUP_INTERVAL_MILLIS = 200L;
    private static final long WARMUP_FIRST_JOB_ID = 1_000_000L;
    private static final long WARMUP_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(2);
    private static final long WARMUP_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    /**
     * How long teardown of the warm-up coordinators waits for scheduler threads they started to
     * exit. A per-pipeline scheduler's threads exit once its coordinator is cancelled; a shared
     * scheduler's threads stay, and the measured coordinators then use them, so they are counted.
     */
    private static final long WARMUP_THREAD_EXIT_NANOS = TimeUnit.SECONDS.toNanos(2);

    private final int pipelineNum;
    private final long intervalMillis;
    private final long intervalNanos;
    private final long warmupTriggers;
    private final AtomicLong triggeredCheckpoints = new AtomicLong();
    private int probeStride;
    private Set<Thread> preexistingSchedulerThreads = Collections.emptySet();

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<FakeTaskCheckpointManager> managers = new ArrayList<>();

    private SeaTunnelStorageEnvironmentContext environment;
    private ThreadPoolExecutor coordinatorExecutor;
    private AtomicInteger[] pendingCounters;
    private long[] lastTriggerNanos;

    private long spinWindowNanos = MIN_SPIN_WINDOW_NANOS;
    private int current = -1;
    private long sampled;
    private long skippedPending;
    private long skippedCollided;
    private long skippedOverrun;
    private long skippedEarly;

    /**
     * @param pipelineNum number of jobs, each with one single-task pipeline
     * @param intervalMillis checkpoint interval of every job
     */
    public CheckpointSchedulingFixture(int pipelineNum, long intervalMillis) {
        this(pipelineNum, intervalMillis, 0L);
    }

    /**
     * @param pipelineNum number of jobs, each with one single-task pipeline
     * @param intervalMillis checkpoint interval of every job
     * @param warmupTriggers checkpoints to trigger on throwaway coordinators before the measured
     *     ones are created, 0 for none; see {@link #warmUpTriggerPath}
     */
    public CheckpointSchedulingFixture(int pipelineNum, long intervalMillis, long warmupTriggers) {
        this.pipelineNum = pipelineNum;
        this.intervalMillis = intervalMillis;
        this.intervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMillis);
        this.warmupTriggers = warmupTriggers;
    }

    /**
     * Starts the member, warms up the trigger path, creates the coordinators and starts their
     * pipelines staggered across one interval, so due triggers arrive as a steady stream rather
     * than a burst. Returns once every coordinator's trigger phase is known.
     */
    public void setUp() throws Exception {
        validateParameters();
        probeStride = probeStride(pipelineNum, intervalNanos);
        preexistingSchedulerThreads = new HashSet<>(liveSchedulerThreads());
        environment = new SchedulingEnvironmentContext();
        environment.setUp();
        SeaTunnelServer server = environment.getServer();
        EngineConfig engineConfig = server.getSeaTunnelConfig().getEngineConfig();
        coordinatorExecutor = createCoordinatorExecutor(engineConfig);
        warmUpTriggerPath(server, engineConfig);
        managers.addAll(
                createManagers(
                        server,
                        engineConfig.getCheckpointConfig(),
                        coordinatorExecutor,
                        failure,
                        FIRST_JOB_ID,
                        pipelineNum,
                        intervalMillis));
        pendingCounters = new AtomicInteger[pipelineNum];
        lastTriggerNanos = new long[pipelineNum];
        for (int i = 0; i < pipelineNum; i++) {
            pendingCounters[i] =
                    readPendingCounter(managers.get(i).getCheckpointCoordinator(PIPELINE_ID));
            lastTriggerNanos[i] = NOT_SYNCED;
        }
        startStaggered(managers, intervalNanos);
        // Every coordinator, not only the probes: once each has triggered, each has armed its
        // scheduler, so countSchedulerThreads() sees the full thread cost of pipelineNum pipelines.
        resync(1);
    }

    /** Resynchronises the coordinators that were taken out of the rotation, then resets counts. */
    public void beginIteration() {
        resync(probeStride);
        sampled = 0;
        skippedPending = 0;
        skippedCollided = 0;
        skippedOverrun = 0;
        skippedEarly = 0;
    }

    /**
     * Picks the coordinator due soonest and returns exactly when its trigger is due, with its
     * previous checkpoint completed and the trigger not yet run. Not measured.
     */
    public void awaitNextDueTrigger() {
        checkFailure();
        while (true) {
            int next = soonestSynced();
            long expected = lastTriggerNanos[next] + intervalNanos;
            long parkTarget = expected - spinWindowNanos;
            long start = System.nanoTime();
            if (start >= expected) {
                // It came due while the previous sample was being taken; it may have run
                // unobserved, so its phase is lost.
                skipCollided(next);
                continue;
            }
            // Close enough to the due time already (the previous sample ended late in this
            // trigger's window): skip parking and go straight to the spin.
            if (start < parkTarget) {
                parkUntil(parkTarget);
                widenSpinWindow(System.nanoTime() - parkTarget);
            }
            // Counter first, then the clock: if the clock is still before the due time, the
            // counter was read before the trigger could have run.
            boolean pending = pendingCounters[next].get() != 0;
            if (System.nanoTime() >= expected) {
                // Parking overshot the due time; the trigger may have run unobserved.
                skipOverrun(next);
                continue;
            }
            if (pending) {
                // A checkpoint is pending before the trigger is due. Usually the previous one is
                // still running and this trigger takes the pending re-arm path; it can also be this
                // trigger having run early against an estimate made from a late observation, when
                // the measuring thread was descheduled. Either way the sample is unusable.
                skipPending(next);
                continue;
            }
            while (System.nanoTime() < expected) {
                // Spin through the last stretch so the measured window starts on time.
            }
            if (pendingCounters[next].get() != 0) {
                // The counter read 0 before the due time and only a trigger raises it, so the
                // trigger ran no later than its estimated due time: the estimate was late.
                skipEarly(next);
                continue;
            }
            current = next;
            return;
        }
    }

    /**
     * Spins until the due coordinator's trigger has created its pending checkpoint. This is the
     * measured part: it starts when the trigger is due and ends when the trigger has run.
     *
     * @return the time the trigger was observed
     */
    public long awaitTrigger() {
        AtomicInteger pendingCounter = pendingCounters[current];
        long deadline = System.nanoTime() + intervalNanos;
        // The window starts slightly before the real deadline, and that is intended. The trigger
        // body increments pendingCounter just before it re-arms the next trigger with
        // schedule(..., interval), so "observed + interval" is a few microseconds early and each
        // sample also includes the tail of the previous trigger body. That code is identical on
        // every scheduler being compared, so the offset is equal on both sides and cancels in a
        // comparison. Do not "fix" it by moving the start later: there is no observable point
        // closer to the real deadline without changing engine code.
        while (pendingCounter.get() == 0) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(
                        "Checkpoint trigger of job "
                                + (FIRST_JOB_ID + current)
                                + " did not run within one interval of being due");
            }
        }
        long observed = System.nanoTime();
        lastTriggerNanos[current] = observed;
        sampled++;
        return observed;
    }

    /**
     * Summarises the iteration's skips. Printed for every iteration, passing or not, so a run close
     * to the limit stays visible.
     */
    public String iterationReport() {
        return String.format(
                "measured %d of %d due triggers; skipped %d with a checkpoint already pending "
                        + "before the due time, %d that came due while another trigger was being "
                        + "measured, %d where parking overran the due time, %d that ran before "
                        + "their estimated due time",
                sampled,
                dueTriggers(),
                skippedPending,
                skippedCollided,
                skippedOverrun,
                skippedEarly);
    }

    /**
     * Rejects the iteration if more than {@link #MAX_SKIP_RATIO} of due triggers could not be
     * measured, so a run where most triggers took the pending re-arm path produces an error rather
     * than a number. Enforced once at least {@link #MIN_DUE_TRIGGERS_FOR_SKIP_CHECK} triggers were
     * due; shorter iterations still print their counts.
     */
    public void endIteration() {
        checkFailure();
        if (isSkipShareTooHigh(dueTriggers(), dueTriggers() - sampled)) {
            throw new IllegalStateException(
                    String.format(
                            "%d pipelines: %s. That is above the %.0f%% limit, so the measured "
                                    + "delays would not be representative",
                            pipelineNum, iterationReport(), MAX_SKIP_RATIO * 100));
        }
    }

    static boolean isSkipShareTooHigh(long due, long skipped) {
        return due >= MIN_DUE_TRIGGERS_FOR_SKIP_CHECK && skipped > MAX_SKIP_RATIO * due;
    }

    private long dueTriggers() {
        return sampled + skippedPending + skippedCollided + skippedOverrun + skippedEarly;
    }

    public void tearDown() throws Exception {
        try {
            // Executor first: a coordinator still finishing its asynchronous start would otherwise
            // arm its first trigger on the scheduler that cancelling just replaced, and that
            // thread would outlive the fixture.
            if (coordinatorExecutor != null) {
                coordinatorExecutor.shutdownNow();
                if (!coordinatorExecutor.awaitTermination(
                        SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Coordinator executor did not stop");
                }
            }
            for (FakeTaskCheckpointManager manager : managers) {
                manager.cancelCheckpoint(PIPELINE_ID);
            }
            managers.clear();
        } finally {
            coordinatorExecutor = null;
            if (environment != null) {
                environment.tearDown();
                environment = null;
            }
        }
    }

    long getSampled() {
        return sampled;
    }

    long getTriggeredCheckpoints() {
        return triggeredCheckpoints.get();
    }

    /**
     * Counts this fixture's live checkpoint scheduler threads. This is the cost a shared scheduler
     * exists to remove, so it is reported rather than derived.
     */
    public long countSchedulerThreads() {
        return schedulerThreads().size();
    }

    /**
     * Live checkpoint scheduler threads started since this fixture's setup began. Threads that
     * already existed, such as ones another test in the same JVM has not finished stopping, are not
     * counted.
     */
    List<Thread> schedulerThreads() {
        return liveSchedulerThreads().stream()
                .filter(thread -> !preexistingSchedulerThreads.contains(thread))
                .collect(Collectors.toList());
    }

    private static List<Thread> liveSchedulerThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith(SCHEDULER_THREAD_NAME_PREFIX))
                .collect(Collectors.toList());
    }

    /**
     * Runs the production trigger path hot before anything is measured, then removes the
     * coordinators that did it.
     *
     * <p>The JIT compiles the trigger path by how often it runs, not by how long the benchmark has
     * been running. Measured points trigger 5 to 50 checkpoints a second, so on its own the path
     * takes minutes to reach fully optimised code, and at one pipeline it does not get there within
     * a whole trial: the score then trends down through every measured iteration. Throwaway
     * coordinators on the same member trigger {@code warmupTriggers} checkpoints first, at the same
     * rate for every point of the sweep, so every point starts measuring on the same compiled code.
     * Their job ids, scheduler threads and pending checkpoints are gone before the measured
     * coordinators are created.
     *
     * <p>They are stopped the way {@link #tearDown} stops the measured ones, executor first, so a
     * checkpoint still starting cannot arm its timeout on the scheduler that cancelling replaces,
     * which would leave that thread running. They have their own executor for that, and their own
     * failure sink: stopping the executor under a running coordinator can fail its checkpoint,
     * which is expected then and must not fail the trial.
     */
    private void warmUpTriggerPath(SeaTunnelServer server, EngineConfig engineConfig)
            throws InterruptedException {
        if (warmupTriggers == 0) {
            return;
        }
        ThreadPoolExecutor warmupExecutor = createCoordinatorExecutor(engineConfig);
        AtomicReference<Throwable> warmupFailure = new AtomicReference<>();
        List<FakeTaskCheckpointManager> warmup =
                createManagers(
                        server,
                        engineConfig.getCheckpointConfig(),
                        warmupExecutor,
                        warmupFailure,
                        WARMUP_FIRST_JOB_ID,
                        WARMUP_PIPELINES,
                        WARMUP_INTERVAL_MILLIS);
        long target = triggeredCheckpoints.get() + warmupTriggers;
        long deadline = System.nanoTime() + WARMUP_TIMEOUT_NANOS;
        try {
            startStaggered(warmup, TimeUnit.MILLISECONDS.toNanos(WARMUP_INTERVAL_MILLIS));
            while (triggeredCheckpoints.get() < target) {
                Throwable throwable = warmupFailure.get();
                if (throwable != null) {
                    throw new IllegalStateException(
                            "Checkpoint coordinator failed during warm-up", throwable);
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException(
                            "Warm-up triggered "
                                    + (triggeredCheckpoints.get() + warmupTriggers - target)
                                    + " of "
                                    + warmupTriggers
                                    + " checkpoints within "
                                    + TimeUnit.NANOSECONDS.toSeconds(WARMUP_TIMEOUT_NANOS)
                                    + " s");
                }
                parkUntil(System.nanoTime() + WARMUP_POLL_NANOS);
            }
        } finally {
            warmupExecutor.shutdownNow();
            if (!warmupExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Warm-up coordinator executor did not stop");
            }
            for (FakeTaskCheckpointManager manager : warmup) {
                manager.cancelCheckpoint(PIPELINE_ID);
            }
        }
        awaitWarmupSchedulerThreadsExit();
    }

    private void awaitWarmupSchedulerThreadsExit() throws InterruptedException {
        long deadline = System.nanoTime() + WARMUP_THREAD_EXIT_NANOS;
        for (Thread thread : schedulerThreads()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
        }
    }

    private List<FakeTaskCheckpointManager> createManagers(
            SeaTunnelServer server,
            CheckpointConfig memberConfig,
            ThreadPoolExecutor executor,
            AtomicReference<Throwable> failureSink,
            long firstJobId,
            int count,
            long jobIntervalMillis) {
        NodeEngine nodeEngine = server.getNodeEngine();
        IMap<Object, Object> runningJobState =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        CheckpointConfig jobConfig = new CheckpointConfig();
        jobConfig.setCheckpointInterval(jobIntervalMillis);
        jobConfig.setStorage(memberConfig.getStorage());

        List<FakeTaskCheckpointManager> created = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long jobId = firstJobId + i;
            created.add(
                    new FakeTaskCheckpointManager(
                            jobId,
                            nodeEngine,
                            singleTaskPlan(jobId),
                            jobConfig,
                            server.getCheckpointService().getCheckpointStorage(),
                            executor,
                            runningJobState,
                            server.getEngineContext(),
                            server.getCheckpointMonitorService(),
                            failureSink,
                            triggeredCheckpoints));
        }
        return created;
    }

    private static void startStaggered(
            List<FakeTaskCheckpointManager> toStart, long staggerIntervalNanos) {
        long start = System.nanoTime();
        for (int i = 0; i < toStart.size(); i++) {
            parkUntil(start + staggerIntervalNanos * i / toStart.size());
            toStart.get(i).startTask();
        }
    }

    /**
     * Spins over every {@code stride}-th coordinator until each one without a known phase has been
     * seen idle and then triggering. Those that already have a phase are refreshed whenever they
     * trigger during the wait, so they do not fall out of the rotation meanwhile. Not measured; it
     * occupies one core for up to a few intervals.
     *
     * @param stride {@code probeStride} to cover the probes, 1 to cover every coordinator
     */
    private void resync(int stride) {
        boolean[] seenIdle = new boolean[pipelineNum];
        long deadline = System.nanoTime() + RESYNC_INTERVALS * intervalNanos + PENDING_REARM_NANOS;
        long covered = strideCount(stride);
        long unsynced = covered - syncedCount(stride);
        while (unsynced > 0) {
            checkFailure();
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(
                        unsynced
                                + " of "
                                + covered
                                + " checkpoint coordinators did not trigger within "
                                + RESYNC_INTERVALS
                                + " intervals plus one pending re-arm");
            }
            for (int i = 0; i < pipelineNum; i += stride) {
                if (pendingCounters[i].get() == 0) {
                    seenIdle[i] = true;
                } else if (seenIdle[i]) {
                    if (lastTriggerNanos[i] == NOT_SYNCED) {
                        unsynced--;
                    }
                    lastTriggerNanos[i] = System.nanoTime();
                    seenIdle[i] = false;
                }
            }
        }
    }

    /**
     * Returns the probe with a known phase that is due soonest. Resynchronises first once fewer
     * than half the probes have a known phase: skipped probes would otherwise stay out of the
     * rotation for the rest of the iteration, and with few probes a single skip can leave none.
     */
    private int soonestSynced() {
        if (syncedCount(probeStride) * 2 < probeCount()) {
            resync(probeStride);
        }
        int soonest = -1;
        for (int i = 0; i < pipelineNum; i += probeStride) {
            if (lastTriggerNanos[i] != NOT_SYNCED
                    && (soonest < 0 || lastTriggerNanos[i] < lastTriggerNanos[soonest])) {
                soonest = i;
            }
        }
        return soonest;
    }

    public long probeCount() {
        return strideCount(probeStride);
    }

    private long strideCount(int stride) {
        return (pipelineNum + stride - 1) / stride;
    }

    private long syncedCount(int stride) {
        long synced = 0;
        for (int i = 0; i < pipelineNum; i += stride) {
            if (lastTriggerNanos[i] != NOT_SYNCED) {
                synced++;
            }
        }
        return synced;
    }

    private void widenSpinWindow(long parkOvershootNanos) {
        spinWindowNanos =
                Math.min(
                        MAX_SPIN_WINDOW_NANOS,
                        Math.max(spinWindowNanos, parkOvershootNanos + MIN_SPIN_WINDOW_NANOS));
    }

    private void skipPending(int coordinator) {
        lastTriggerNanos[coordinator] = NOT_SYNCED;
        skippedPending++;
    }

    private void skipCollided(int coordinator) {
        lastTriggerNanos[coordinator] = NOT_SYNCED;
        skippedCollided++;
    }

    private void skipOverrun(int coordinator) {
        lastTriggerNanos[coordinator] = NOT_SYNCED;
        skippedOverrun++;
    }

    private void skipEarly(int coordinator) {
        lastTriggerNanos[coordinator] = NOT_SYNCED;
        skippedEarly++;
    }

    private void checkFailure() {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throw new IllegalStateException("Checkpoint coordinator failed", throwable);
        }
    }

    private void validateParameters() {
        if (pipelineNum < 1) {
            throw new IllegalArgumentException("pipelineNum must be at least 1");
        }
        if (intervalMillis < MIN_CHECKPOINT_INTERVAL_MILLIS) {
            throw new IllegalArgumentException(
                    "checkpoint interval must be at least "
                            + MIN_CHECKPOINT_INTERVAL_MILLIS
                            + " ms to match the minimum SeaTunnel accepts");
        }
    }

    /**
     * Starts are spread evenly across one interval, so coordinators {@code i} and {@code i +
     * stride} start {@code stride * interval / pipelineNum} apart. Returns the smallest stride that
     * keeps that at least {@link #MIN_PROBE_SPACING_NANOS}, and at most {@code pipelineNum} so
     * there is always one probe.
     */
    static int probeStride(int pipelineNum, long intervalNanos) {
        long stride = (MIN_PROBE_SPACING_NANOS * pipelineNum + intervalNanos - 1) / intervalNanos;
        return (int) Math.max(1L, Math.min(stride, pipelineNum));
    }

    private static CheckpointPlan singleTaskPlan(long jobId) {
        TaskLocation task = new TaskLocation(new TaskGroupLocation(jobId, PIPELINE_ID, 1L), 0, 0);
        return CheckpointPlan.builder()
                .pipelineId(PIPELINE_ID)
                .pipelineSubtasks(Collections.singleton(task))
                .startingSubtasks(Collections.singleton(task))
                .pipelineActions(Collections.emptyMap())
                .subtaskActions(Collections.emptyMap())
                .build();
    }

    /** Same shape as the executor {@code CoordinatorService} hands every {@code JobMaster}. */
    private static ThreadPoolExecutor createCoordinatorExecutor(EngineConfig engineConfig) {
        AtomicInteger threadIndex = new AtomicInteger();
        return new ThreadPoolExecutor(
                engineConfig.getCoordinatorServiceConfig().getCoreThreadNum(),
                engineConfig.getCoordinatorServiceConfig().getMaxThreadNum(),
                EXECUTOR_KEEP_ALIVE_SECONDS,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName(
                            "benchmark-coordinator-service-" + threadIndex.getAndIncrement());
                    return thread;
                });
    }

    private static AtomicInteger readPendingCounter(CheckpointCoordinator coordinator) {
        try {
            return (AtomicInteger) PENDING_COUNTER_FIELD.get(coordinator);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot read the pending checkpoint counter", e);
        }
    }

    /**
     * The storage benchmarks' member, with its checkpoint storage isolated in the trial's temporary
     * directory, but without IMap persistence. Its write-through MapStore puts a file write on a
     * partition thread for every engine IMap update, including checkpoint id allocation, which
     * stalls checkpoints for hundreds of milliseconds and is not what this benchmark measures.
     */
    private static final class SchedulingEnvironmentContext
            extends SeaTunnelStorageEnvironmentContext {

        private static final String ENGINE_MAPS = "engine*";

        @Override
        protected SeaTunnelConfig createSeaTunnelConfig(String clusterName) {
            SeaTunnelConfig config = super.createSeaTunnelConfig(clusterName);
            config.getHazelcastConfig()
                    .getMapConfig(ENGINE_MAPS)
                    .getMapStoreConfig()
                    .setEnabled(false);
            return config;
        }
    }

    private static void parkUntil(long deadlineNanos) {
        long remaining;
        while ((remaining = deadlineNanos - System.nanoTime()) > 0) {
            LockSupport.parkNanos(remaining);
        }
    }
}
