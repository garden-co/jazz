import gzip, os, re, sys, glob
root = sys.argv[1]
nx = os.path.join(root, ".next")
def gz(p): return len(gzip.compress(open(p,'rb').read(), 9))
for html in sorted(glob.glob(nx + "/server/app/**/*.html", recursive=True)):
    route = "/" + os.path.relpath(html, nx + "/server/app")[:-5].replace("index", "")
    srcs = sorted(set(re.findall(r'src="(/_next/static/[^"]+\.js)"', open(html).read())))
    raw = sum(os.path.getsize(nx + s[len('/_next'):]) for s in srcs)
    g = sum(gz(nx + s[len('/_next'):]) for s in srcs)
    print(f"route {route:20s} first-load JS: {len(srcs)} chunks, {raw/1024:.1f} KiB raw, {g/1024:.1f} KiB gzip")
tot = 0
for p in sorted(glob.glob(nx + "/static/chunks/**/*.js", recursive=True)):
    tot += os.path.getsize(p)
print(f"all static chunks: {tot/1024:.1f} KiB raw")
for w in sorted(glob.glob(nx + "/**/*.wasm", recursive=True)):
    print(f"wasm {os.path.relpath(w, root)}: {os.path.getsize(w)} B ({os.path.getsize(w)/1048576:.2f} MiB), gzip {gz(w)} B ({gz(w)/1048576:.2f} MiB)")
# which chunks reference the wasm
for p in glob.glob(nx + "/static/chunks/**/*.js", recursive=True):
    if "jazz_wasm_bg" in open(p, errors="ignore").read():
        print("wasm referenced by chunk", os.path.relpath(p, nx), os.path.getsize(p))
