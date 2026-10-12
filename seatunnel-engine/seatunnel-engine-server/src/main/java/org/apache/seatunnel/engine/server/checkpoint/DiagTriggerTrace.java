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

package org.apache.seatunnel.engine.server.checkpoint;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * THROWAWAY diagnostic for apache/seatunnel#12418, fork branch bench-diag-12418 only. Records, per
 * periodic trigger: the exact due time, when the scheduler thread started the trigger body, and
 * when pendingCounter was incremented. Enabled by -Ddiag.trace.dir; written at JVM exit.
 */
public final class DiagTriggerTrace {

    private static final String DIR = System.getProperty("diag.trace.dir");
    private static final boolean ON = DIR != null;
    private static final long BASE_NANOS = System.nanoTime();
    private static final long BASE_UPTIME_MILLIS =
            ManagementFactory.getRuntimeMXBean().getUptime();
    private static final ThreadLocal<long[]> CURRENT = ThreadLocal.withInitial(() -> new long[2]);
    private static final ConcurrentLinkedQueue<long[]> ROWS = new ConcurrentLinkedQueue<>();

    static {
        if (ON) {
            Runtime.getRuntime().addShutdownHook(new Thread(DiagTriggerTrace::dump));
        }
    }

    private DiagTriggerTrace() {}

    static void start(long dueNanos) {
        if (ON) {
            long[] current = CURRENT.get();
            current[0] = dueNanos;
            current[1] = System.nanoTime();
        }
    }

    static void increment(long jobId) {
        if (ON) {
            long now = System.nanoTime();
            long[] current = CURRENT.get();
            if (current[1] != 0) {
                ROWS.add(new long[] {jobId, current[0], current[1], now});
                current[1] = 0;
            }
        }
    }

    private static void dump() {
        String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        File file = new File(DIR, "trace-" + pid + ".csv");
        try (PrintWriter out = new PrintWriter(new FileWriter(file))) {
            out.println("# base_nanos=" + BASE_NANOS + " base_uptime_ms=" + BASE_UPTIME_MILLIS);
            out.println("job_id,due_ns,start_ns,increment_ns");
            for (long[] row : ROWS) {
                out.println(row[0] + "," + row[1] + "," + row[2] + "," + row[3]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
