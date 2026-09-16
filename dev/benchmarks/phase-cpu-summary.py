import argparse, bisect, collections, heapq, json, re
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('timeline', type=Path)
p.add_argument('perf_script', type=Path)
p.add_argument('--kernel-symbols', type=Path)
a=p.parse_args()
doc=json.loads(a.timeline.read_text())
assert doc['clock']=='CLOCK_MONOTONIC', 'capture perf with --clockid mono'
assert doc['dropped_intervals']==0, 'timeline truncated; do not attribute this capture'
assert doc['columns']==['start_ns','end_ns','phase','role','depth']
intervals=sorted(doc['intervals']); owner=doc['owner_tid']
kernel=[]
if a.kernel_symbols:
 for line in a.kernel_symbols.open():
  parts=line.split()
  if len(parts)>=3:
   address=int(parts[0],16)
   if address>=0xffff000000000000: kernel.append((address,parts[2]))
 kernel.sort()
addresses=[x[0] for x in kernel]
samples=[]; current=None
for line in a.perf_script.open():
 m=re.match(r'\s*(\d+)/(\d+)\s+(\d+)\.(\d+):\s+(\d+)\s+cycles',line)
 if m:
  current={'tid':int(m[2]),'time':int(m[3])*10**9+int(m[4].ljust(9,'0')),'period':int(m[5]),'stack':[]}
  samples.append(current)
 elif current is not None and line.startswith('\t'):
  m=re.match(r'\s*([0-9a-f]+)\s+(.*?)\s+\([^)]*\)\s*$',line)
  if m:
   name=m[2]; addr=int(m[1],16)
   if name=='[unknown]' and kernel and addr>=0xffff000000000000:
    i=bisect.bisect_right(addresses,addr)-1
    if i>=0 and addr-addresses[i]<65536: name='kernel:'+kernel[i][1]
   current['stack'].append(name)
assert samples, 'no samples; use perf script --ns -F pid,tid,time,period,event,ip,sym,dso'
phases=collections.Counter(); counts=collections.Counter(); leaves=collections.defaultdict(collections.Counter)
active=[]; index=0
for sample in sorted(samples,key=lambda s:s['time']):
 t=sample['time']
 while index<len(intervals) and intervals[index][0]<=t:
  start,end,phase,role,depth=intervals[index]
  if end>t: heapq.heappush(active,(-depth,end,index,phase,role))
  index+=1
 while active and active[0][1]<=t: heapq.heappop(active)
 if sample['tid']!=owner: name='other threads'
 elif not active: name='outside recorded phases'
 else:
  _,_,_,phase,role=active[0]
  name=doc['roles'][role]+'/'+doc['phases'][phase]
 period=sample['period']; phases[name]+=period; counts[name]+=1
 leaves[name][sample['stack'][0] if sample['stack'] else '[empty stack]']+=period
 sample['phase']=name
print('Samples:',len(samples),'empty stacks:',sum(not s['stack'] for s in samples),'intervals:',len(intervals))
total=sum(phases.values())
print('Exclusive phase attribution (% of all sampled cycles; not wall time):')
for name,period in phases.most_common():
 print(f'{100*period/total:6.2f}% {counts[name]:5d} samples {name}')
for name,_ in phases.most_common(12):
 print('\n'+name+' — leading self frames (% within phase):')
 for leaf,period in leaves[name].most_common(10): print(f'{100*period/phases[name]:6.2f}% {leaf}')
