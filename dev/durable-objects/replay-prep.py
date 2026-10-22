"""Group a JAZZ_SQL_TRACE file into replay jobs for replay.js.

Usage: python3 replay-prep.py TRACE.jsonl JOBS.json

One job per connection: its DDL runs in order; every other statement is
prepared (EXPLAIN) once per schema state. Transaction statements and PRAGMAs
are counted, not replayed: Durable Objects refuse them all. Also reports
what limits cannot show at prepare time: values over 2 MiB and integers
beyond 2^53.
"""
import json, re, collections, hashlib, sys
TRACE, JOBS = sys.argv[1], sys.argv[2]
DDL = re.compile(r'\s*(CREATE|ALTER|DROP)\b', re.I)
TXN = re.compile(r'\s*(SAVEPOINT|RELEASE|ROLLBACK|BEGIN|COMMIT|END)\b', re.I)
PRAGMA = re.compile(r'\s*PRAGMA\b', re.I)
conns = collections.OrderedDict()
stats = collections.Counter(); maxval = collections.Counter(); bigint = collections.Counter()
params_over = collections.Counter(); runs = collections.Counter()
for line in open(TRACE):
    try: r = json.loads(line)
    except Exception: stats['bad'] += 1; continue
    s = r['sql']; c = r['conn']; runs[s] += 1
    conns.setdefault(c, []).append((s, r['params']))
    if r['max_value'] > 2 * 1024 * 1024: maxval[s] = max(maxval[s], r['max_value'])
    if r['big_int']: bigint[s] += 1
    if r['params'] > 100: params_over[s] = max(params_over[s], r['params'])
# group connections by schema state; emit ops deduped per (schema-hash, sql)
jobs = {}; seen = set(); static = collections.Counter()
for c, stmts in conns.items():
    h = hashlib.sha1(); ops = []
    for s, p in stmts:
        if TXN.match(s): static[('txn', re.match(r'\s*(\w+\s*\w*)', s).group(1).upper())] += 1; continue
        if PRAGMA.match(s): static[('pragma', s.strip()[:80])] += 1; continue
        if DDL.match(s):
            h.update(s.encode()); ops.append(('ddl', s, p)); continue
        key = (h.hexdigest(), s)
        if key in seen: continue
        seen.add(key); ops.append(('explain', s, p))
    # job key: full ddl sequence + ops; merge identical ddl prefixes by job of first conn
    jobs[c] = ops
# collapse jobs whose ops (ddl+explains) are empty of explains
out = [ops for ops in jobs.values() if any(o[0] == 'explain' for o in ops)]
json.dump(out, open(JOBS, 'w'))
print('conns', len(conns), 'jobs', len(out), 'ops', sum(len(o) for o in out), 'unique sql', len(runs))
print('static', static.most_common())
print('params>100', len(params_over), sorted(params_over.values())[-5:])
for s, n in params_over.most_common(8): print('  ', n, s[:150])
print('values>2MiB', len(maxval)); [print('  ', v, s[:150]) for s, v in maxval.most_common(8)]
print('bigint', len(bigint)); [print('  ', v, s[:200]) for s, v in bigint.most_common(10)]
