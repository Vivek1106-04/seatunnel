# Holds /dev/cpu_dma_latency at 0 us while running: idle CPUs poll instead of halting the vCPU.
import os, struct, time
fd = os.open('/dev/cpu_dma_latency', os.O_WRONLY)
os.write(fd, struct.pack('i', 0))
print('cpu_dma_latency held at 0', flush=True)
while True:
    time.sleep(3600)
