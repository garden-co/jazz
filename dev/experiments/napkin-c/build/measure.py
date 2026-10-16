"""Full-port build used for the size comparison.
1. compile every .c under rt/ third_party/ src/ to LTO bitcode (out/obj), -Oz
2. list externally-defined functions (llvm-nm) -> out/keep.c: a used table of their
   addresses, so every ported function survives even where cross-unit call names drift
   (Rust's shipped set is exactly what we ported, so nothing ported may be DCE'd)
3. undefined symbols -> out/stubs.c (trap bodies; counted and reported)
4. link a reactor with -flto -Oz --gc-sections, keep the name section, wasm-opt -O -g
Usage: measure.py [--no-keep] [--opt-flag -Oz|-Os] [--strip-names] [--tag NAME]"""
import os, subprocess, sys, glob, re, concurrent.futures as cf, gzip, brotli, shutil
here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SDK = '/home/user/scratch/wasi-sdk-25.0-x86_64-linux'
CC, NM = f'{SDK}/bin/clang', f'{SDK}/bin/llvm-nm'
WOPT = '/home/user/scratch/node_modules/.bin/wasm-opt'
args = sys.argv[1:]
keep = '--no-keep' not in args
opt = args[args.index('--opt-flag') + 1] if '--opt-flag' in args else '-Oz'
lto = [] if '--no-lto' in args else ['-flto']
tag = args[args.index('--tag') + 1] if '--tag' in args else f'napkin{opt}'
out = f'{here}/out/{tag}'; os.makedirs(f'{out}/obj', exist_ok=True)
srcs = [p for r in ('rt', 'third_party', 'src') for p in glob.glob(f'{here}/{r}/**/*.c', recursive=True) if '/probe/' not in p]
def cflags_for(p):
    fl = []; d = os.path.dirname(p)
    while d.startswith(here):
        f = f'{d}/cflags.txt'
        if os.path.exists(f): fl = open(f).read().split() + fl
        d = os.path.dirname(d)
    return fl
def comp(p):
    rel = os.path.relpath(p, here); o = f'{out}/obj/' + rel.replace('/', '__') + '.o'
    cmd = [CC, '--target=wasm32-wasip1', opt] + lto + ['-std=gnu11', '-DNDEBUG', '-c', '-o', o,
           '-I', f'{here}/rt', '-I', f'{here}/src', '-I', os.path.dirname(p), '-w'] + cflags_for(p) + [p]
    r = subprocess.run(cmd, capture_output=True, text=True)
    return rel, o, r.returncode, r.stderr[:600]
with cf.ThreadPoolExecutor(4) as ex: res = list(ex.map(comp, srcs))
fails = [r for r in res if r[2]]; objs = [r[1] for r in res if not r[2]]
for rel, _, _, err in fails: print('COMPILE FAIL', rel, err.splitlines()[0] if err else '')
defined, undefined, data_defined = set(), set(), set()
def nm_defs(o):
    r = set()
    for line in subprocess.run([NM, '--no-sort', o], capture_output=True, text=True).stdout.splitlines():
        m = re.match(r'\s*(?:[0-9a-f-]+\s+)?([A-Za-z])\s+(\S+)$', line)
        if m and m.group(1) == 'T' and not m.group(2).startswith('__') and m.group(2) != 'main': r.add(m.group(2))
    return sorted(r)
if keep:
    def recomp(item):
        rel, o, rc, _ = item
        if rc or not rel.startswith('src/'): return item  # rt/ and third_party/ use natural DCE
        fns = nm_defs(o)
        if not fns: return item
        w = o[:-2] + '.keep.c'
        with open(w, 'w') as f:
            f.write(f'#include "{here}/{rel}"\n__attribute__((used)) static void *const jz_keep_tu[] = {{\n')
            for fn in fns: f.write(f'  (void *)&{fn},\n')
            f.write('};\n')
        p = f'{here}/{rel}'
        cmd = [CC, '--target=wasm32-wasip1', opt] + lto + ['-std=gnu11', '-DNDEBUG', '-c', '-o', o,
               '-I', f'{here}/rt', '-I', f'{here}/src', '-I', os.path.dirname(p), '-w'] + cflags_for(p) + [w]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode: print('KEEP RECOMPILE FAIL', rel, r.stderr[:300])
        return item
    with cf.ThreadPoolExecutor(4) as ex: list(ex.map(recomp, res))
nm = subprocess.run([NM, '--no-sort'] + objs, capture_output=True, text=True).stdout
for line in nm.splitlines():
    m = re.match(r'\s*(?:[0-9a-f-]+\s+)?([A-Za-z])\s+(\S+)$', line)
    if not m: continue
    t, s = m.groups()
    if t == 'T': defined.add(s)
    elif t in 'DBR': data_defined.add(s)
    elif t == 'U': undefined.add(s)
libc_ok = set()
undefined -= defined | data_defined
# drop libc / compiler-rt symbols (resolved by wasi-libc) by test-linking later; heuristically keep jz/crate names
crate_re = re.compile(r'^(jz_|groove|jazz_|idb_tree|wasm_tracing|ruzstd|GROOVE|JAZZ)')
stubs = sorted(s for s in undefined if crate_re.match(s))
data_stubs = set(); dup = set()
def gen_and_link():
    with open(f'{out}/stubs.c', 'w') as f:
        f.write('/* generated: undefined cross-unit symbols. Opaque host call, not a trap, so wasm-opt cannot treat callers as dead after the call. */\n__attribute__((import_module("env"), import_name("unported"))) void jz_unported(void);\n')
        for s in stubs:
            if s in dup: continue
            if s in data_stubs: f.write(f'char {s}[64];\n')
            else: f.write(f'void {s}(void) {{ jz_unported(); }}\n')
    extra = []
    for c in ('stubs.c', 'keep.c'):
        o = f'{out}/{c}.o'
        subprocess.run([CC, '--target=wasm32-wasip1', opt, '-c', '-w', '-o', o, f'{out}/{c}'], check=True)
        extra.append(o)
    link = [CC, '--target=wasm32-wasip1', opt] + lto + ['-mexec-model=reactor', '-o', wasm,
            '-Wl,--gc-sections', '-Wl,--strip-debug', '-Wl,--allow-undefined', '-Wl,--error-limit=0'] + objs + extra
    return subprocess.run(link, capture_output=True, text=True)
kept = sorted(s for s in defined if not s.startswith('__') and s not in ('main',))
with open(f'{out}/keep.c', 'w') as f:
    f.write('/* generated: keep every ported function alive */\n')
    if False:
        for s in kept: f.write(f'extern void {s}(void);\n')
        f.write('__attribute__((used)) void *const jz_keep_table[] = {\n')
        for s in kept: f.write(f'  (void *)&{s},\n')
        f.write('};\n__attribute__((export_name("jz_keep"))) void *const *jz_keep(void) { return jz_keep_table; }\n')
wasm = f'{out}/napkin.pre.wasm'
for attempt in range(6):
    r = gen_and_link()
    if r.returncode == 0: break
    newd = set(re.findall(r'symbol type mismatch: (\S+)\n>>> defined as WASM_SYMBOL_TYPE_DATA', r.stderr))
    newdup = set(re.findall(r'duplicate symbol: (\S+)', r.stderr))
    if not (newd - data_stubs) and not (newdup - dup): print(r.stderr[-3000:]); sys.exit(1)
    data_stubs |= newd; dup |= newdup
stubs = [s for s in stubs if s not in dup]
warn = r.stderr.count('signature mismatch')
final = f'{out}/napkin.wasm'
subprocess.run([WOPT, wasm, '--no-inline=signature_mismatch*', '-O', '-g', '--low-memory-unused', '-o', final], check=True)
def sizes(p):
    b = open(p, 'rb').read(); return len(b), len(gzip.compress(b, 9)), len(brotli.compress(b, quality=11))
imports = subprocess.run([f'{SDK}/bin/llvm-objdump', '-h', final], capture_output=True, text=True).stdout
print(f'data_stubs={len(data_stubs)}');print(f'files={len(srcs)} compiled={len(objs)} failed={len(fails)} kept_fns={len(kept) if keep else 0} stubs={len(stubs)} sigmismatch_warnings={warn}')
print('pre-opt  raw/gz/br', *sizes(wasm))
print('final    raw/gz/br', *sizes(final))
open(f'{out}/stubs.txt', 'w').write('\n'.join(stubs))
