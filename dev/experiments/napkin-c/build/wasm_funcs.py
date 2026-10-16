"""Per-function body sizes + section sizes of a wasm module, using its name section.
Usage: wasm_funcs.py FILE.wasm [--csv OUT.csv]"""
import sys, struct
def leb(b, i):
    r = s = 0
    while True:
        x = b[i]; i += 1; r |= (x & 0x7f) << s; s += 7
        if x < 0x80: return r, i
def parse(path):
    b = open(path, 'rb').read(); i = 8; secs = []; names = {}; bodies = []; nimp = 0
    while i < len(b):
        sid = b[i]; i += 1; size, i = leb(b, i); start = i; end = i + size
        sname = None
        if sid == 0:
            nl, j = leb(b, i); sname = b[j:j+nl].decode(); j += nl
            if sname == 'name':
                while j < end:
                    sub = b[j]; j += 1; sl, j = leb(b, j); se = j + sl
                    if sub == 1:
                        n, j = leb(b, j)
                        for _ in range(n):
                            idx, j = leb(b, j); l, j = leb(b, j); names[idx] = b[j:j+l].decode('utf8', 'replace'); j += l
                    j = se
        elif sid == 2:
            n, j = leb(b, i)
            for _ in range(n):
                l, j = leb(b, j); j += l; l, j = leb(b, j); j += l
                k = b[j]; j += 1
                if k == 0: nimp += 1; _, j = leb(b, j)
                elif k == 1: j += 1; fl = b[j]; j += 1; _, j = leb(b, j); j = leb(b, j)[1] if fl & 1 else j
                elif k == 2: fl = b[j]; j += 1; _, j = leb(b, j); j = leb(b, j)[1] if fl & 1 else j
                elif k == 3: j += 2
        elif sid == 10:
            n, j = leb(b, i)
            for k in range(n):
                l, j = leb(b, j); bodies.append(l); j += l
        secs.append((sid, sname, size)); i = end
    funcs = [(names.get(nimp + k, f'f{nimp+k}'), sz) for k, sz in enumerate(bodies)]
    return secs, funcs
if __name__ == '__main__':
    secs, funcs = parse(sys.argv[1])
    SN = {0:'custom',1:'type',2:'import',3:'function',4:'table',5:'memory',6:'global',7:'export',8:'start',9:'elem',10:'code',11:'data',12:'datacount'}
    for sid, n, sz in secs: print(f'{SN.get(sid,sid)}{"("+n+")" if n else ""}\t{sz}')
    print('functions', len(funcs), 'code bytes', sum(s for _, s in funcs))
    if '--csv' in sys.argv:
        import csv
        w = csv.writer(open(sys.argv[sys.argv.index('--csv')+1], 'w'))
        w.writerow(['name', 'body_bytes'])
        for n, s in funcs: w.writerow([n, s])
