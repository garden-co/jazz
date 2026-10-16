"""Bucket C wasm function bytes by source crate (via nm of the per-file objects). Usage: classify_c.py OUTDIR"""
import csv, re, sys, glob, subprocess, collections, os
out = sys.argv[1]
NM = '/home/user/scratch/wasi-sdk-25.0-x86_64-linux/bin/llvm-nm'
sym2file = {}
for o in glob.glob(f'{out}/obj/*.o'):
    rel = os.path.basename(o)[:-2].replace('__', '/')
    for line in subprocess.run([NM, o], capture_output=True, text=True).stdout.splitlines():
        m = re.match(r'\s*(?:[0-9a-f-]+\s+)?([tT])\s+(\S+)$', line)
        if m: sym2file.setdefault(m.group(2), rel)
def bucket(rel):
    p = rel.split('/')
    if p[0] == 'rt': return 'runtime (std/serde/collections equivalent)'
    if p[0] == 'third_party': return 'third_party/' + p[1]
    if p[0] == 'src': return p[1]
    return p[0]
by = collections.Counter(); tot = 0; unk = collections.Counter()
for r in csv.DictReader(open(f'{out}/funcs.csv')):
    n, b = r['name'], int(r['body_bytes']); tot += b
    base = re.sub(r'\.(llvm|lto)\..*$', '', n)
    f = sym2file.get(base)
    if f: k = bucket(f)
    elif n.startswith('signature_mismatch'): k = 'link stubs'
    elif re.match(r'^(jz_|groove|jazz_|idb_tree|wasm_tracing|ruzstd|GROOVE|JAZZ)', base): k = 'unresolved-name stubs/other own'
    else: k = 'libc (wasi-libc: printf, dlmalloc, strtod, ...)'
    by[k] += b
print('total code bytes', tot)
for k, v in by.most_common(): print(f'{v:>10} {v/tot:6.1%} {k}')
