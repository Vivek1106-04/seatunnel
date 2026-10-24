#!/usr/bin/env bash
# Pins the JMH measuring thread (comm "org.apache.seat", the busy-spinning jmh-worker) of every
# forked benchmark JVM to CPU 3 and every other thread of that JVM to CPUs 0-2, re-checking new
# threads every 200 ms. Each tid is pinned once.
declare -A seen
while sleep 0.2; do
  for pid in $(pgrep -f org.openjdk.jmh.runner.ForkedMain); do
    for t in /proc/$pid/task/*; do
      tid=${t##*/}
      [ -n "${seen[$tid]}" ] && continue
      comm=$(cat $t/comm 2>/dev/null) || continue
      if [[ $comm == org.apache.seat* ]]; then cpus=3; else cpus=0-2; fi
      taskset -pc $cpus $tid >/dev/null 2>&1 && seen[$tid]=1 && [ $cpus = 3 ] && echo "$(date +%T) spinner $pid/$tid -> cpu 3"
    done
  done
done
