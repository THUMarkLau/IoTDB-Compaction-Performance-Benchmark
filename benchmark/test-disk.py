import time
import sys

init = False
disk_name = sys.argv[1]
write_speeds = []
read_speeds = []
while True:
    with open("/proc/diskstats", "r") as f:
        msg = f.read()
    msgs = msg.split("\n")
    for msg in msgs:
        if msg.find(disk_name) != -1:
            r = msg.split(" ")
            while "" in r:
                r.remove("")
            if not init:
                prev_r = r[3:]
                init = True
                break
            else:
                r = r[3:]
                read_speed = int(r[2]) * 512 / 1024 / 1024 - int(prev_r[2]) * 512 / 1024 / 1024
                write_speed = int(r[6]) * 512 / 1024 / 1024 - int(prev_r[6]) * 512 / 1024 / 1024
                read_speeds.append(read_speed)
                write_speeds.append(write_speed)
                prev_r = r
            time.sleep(1)
