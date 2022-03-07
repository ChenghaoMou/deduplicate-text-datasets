# Copyright 2021 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import time
import sys
import time

data_size = os.path.getsize(sys.argv[1])

HACK = 100000


started = []

if data_size > 10e9:
    total_jobs = 256
    jobs_at_once = 20
else:
    total_jobs = 96
    jobs_at_once = 96
    

S = data_size//total_jobs

from tqdm import tqdm 

pbar = tqdm(range(0, total_jobs, jobs_at_once))
for jobstart in pbar:
    wait = []
    for i in tqdm(range(jobstart, jobstart+jobs_at_once), leave=False):
        s, e = i*S, min((i+1)*S+HACK, data_size)
        x = "%s.%d-%d"%(sys.argv[1], s, e)
        if os.path.exists(x) and os.path.exists(x+".table.bin") and os.path.getsize(x) != 0 and os.path.getsize(x)*8 == os.path.getsize(x+".table.bin"):
            started.append((s, e))
            if e == data_size:
                break
            else:
                continue
        pbar.set_description(f"Processing {(e - s)/1024**3:.3f} GB {jobstart}")
        cmd = "./target/debug/dedup_dataset save_part %s %d %d"%(sys.argv[1], s, e)
        started.append((s, e))
        wait.append(os.popen(cmd))
        if e == data_size:
            break
    [x.read() for x in wait]

print("Checking all wrote correctly")
files = ["%s.%d-%d"%(sys.argv[1], s, e) for s,e in started]
assert max(x[1] for x in started) == data_size, f"{max(x[1] for x in started)} != {data_size}"

while True:    
    wait = []
    for x,(s,e) in zip(files,started):
        if not os.path.exists(x) or not os.path.exists(x+".table.bin") or os.path.getsize(x) == 0 or os.path.getsize(x)*8 != os.path.getsize(x+".table.bin"):
            cmd = "./target/debug/dedup_dataset save_part %s %d %d"%(sys.argv[1], s, e)
            print(cmd)
            wait.append(os.popen(cmd))
    print("Rerunning", len(wait), "jobs because they failed.")
    [x.read() for x in wait]
    time.sleep(1)
    if len(wait) == 0:
        break
        

print(f"Merging {len(files)} {len(started)} suffix trees")

torun = " ".join(files)
os.popen("./target/debug/dedup_dataset merge_parallel %s tmp/out"%torun).read()
print("Now merging individual tables")
os.popen("cat tmp/out.table.bin.* > tmp/out.table.bin").read()
print("Cleaning up")
os.popen("rm tmp/out.table.bin.*").read()
os.popen("mv tmp/out.table.bin %s.table.bin"%sys.argv[1]).read()

