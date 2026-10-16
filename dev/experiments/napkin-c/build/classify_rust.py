"""Bucket Rust wasm function bytes: crate of origin x kind. Usage: classify_rust.py funcs.csv"""
import csv, re, sys, collections
OWN = ['jazz_node','groove','jazz_db','jazz_model','jazz_engine','jazz_protocol','jazz_types','jazz_wasm','idb_tree','jazz_compression','wasm_tracing']
def kind(n):
    if 'drop_in_place' in n: return 'drop glue'
    if re.search(r'core::slice::sort|slice::sort', n): return 'sort monomorphs'
    if 'alloc::collections::btree' in n or 'BTreeMap' in n.split('::')[0]: return 'BTree monomorphs'
    if 'hashbrown' in n: return 'hashbrown monomorphs'
    if re.search(r'serde|postcard', n): return 'serde/postcard'
    if re.search(r'core::fmt|as core::fmt::(Debug|Display)>', n): return 'fmt/Debug/Display'
    if 'as core::clone::Clone' in n: return 'Clone'
    if re.search(r'as core::cmp::(Ord|PartialOrd|PartialEq)|as core::hash::Hash', n): return 'Eq/Ord/Hash'
    if '{{closure}}' in n: return 'async/closure bodies'
    return 'plain fns'
def crate(n):
    m = re.match(r'[<&\s]*(?:dyn |mut |impl )?([A-Za-z_][A-Za-z0-9_]*)', n)
    c = m.group(1) if m else '?'
    if c in OWN: return c
    for o in OWN:  # generic instance attributed to own crate if it names one
        if o + '::' in n: return o + ' (via std generic)' if c in ('core','alloc','std','hashbrown') else c
    return c
rows = list(csv.DictReader(open(sys.argv[1])))
by_c = collections.Counter(); by_k = collections.Counter(); by_ck = collections.Counter()
tot = 0
for r in rows:
    n, b = r['name'], int(r['body_bytes']); tot += b
    c, k = crate(n), kind(n)
    by_c[c] += b; by_k[k] += b; by_ck[(c, k)] += b
print('total code bytes', tot)
print('\n## by kind'); [print(f'{v:>10} {v/tot:6.1%} {k}') for k, v in by_k.most_common()]
print('\n## by crate (top 40)'); [print(f'{v:>10} {v/tot:6.1%} {k}') for k, v in by_c.most_common(40)]
print('\n## own crates x kind')
for o in OWN:
    ks = {k: v for (c, k), v in by_ck.items() if c == o}
    if ks: print(o, sum(ks.values()), ' '.join(f'{k}={v}' for k, v in sorted(ks.items(), key=lambda x: -x[1])))
