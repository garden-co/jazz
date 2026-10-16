#!/usr/bin/env bash
# Build the napkin C port into a WASM reactor module and report its size.
#
#   build/link.sh [options]
#
# Compiles every .c under rt/, third_party/ and src/ (recursively) with wasi-sdk clang
# (-Oz -flto, bitcode objects), links them into out/napkin.wasm (reactor, --gc-sections,
# --strip-debug but keeping the name section), runs wasm-opt -O -g --low-memory-unused and
# prints raw / gzip -9 / brotli -11 sizes before and after wasm-opt.
#
# Files that fail to compile are reported and skipped (the port is in progress).
# Undefined symbols make the link fail and are listed (all of them go to out/undefined.txt)
# unless --allow-undefined is given, in which case they become imports (all module imports
# are then listed in out/imports.txt).
#
# A file named cflags.txt in a source directory (or any parent directory below the napkin
# root) adds its contents to the compile flags of every .c below it (used by third_party/zstd
# and third_party/blake3 for their size/portability configuration). An --extra file outside
# the napkin tree uses only a cflags.txt in its own directory (e.g. -fno-lto for a probe
# harness, so the runtime is measured behind a real call boundary).
#
# Options:
#   --allow-undefined     pass -Wl,--allow-undefined (undefined symbols become imports)
#   --roots "A B ..."     source roots relative to the napkin dir (default: "rt third_party src")
#   --extra FILE.c        also compile FILE.c (repeatable; e.g. a test harness with exports)
#   --cflags "FLAGS"      extra compile flags for every file (e.g. "-DJZ_RT_PROBE -DP_ALL")
#   --exclude PATTERN     skip sources whose relative path matches the glob (repeatable)
#   --out PATH            output wasm (default: out/napkin.wasm)
#   --name NAME           label printed in the size table (default: basename of --out)
#   -j N                  parallel compile jobs (default: 2)
#   --no-opt              skip wasm-opt
#   --quiet               only print the size summary and failures
set -uo pipefail

here=$(cd "$(dirname "$0")/.." && pwd)
WASI_SDK=${WASI_SDK:-/home/user/scratch/wasi-sdk-25.0-x86_64-linux}
CC="$WASI_SDK/bin/clang"
WASM_OPT=${WASM_OPT:-/home/user/scratch/node_modules/.bin/wasm-opt}

allow_undef=0
roots="rt third_party src"
extras=()
user_cflags=""
excludes=()
out="$here/out/napkin.wasm"
name=""
jobs=2
do_opt=1
quiet=0
while [[ $# -gt 0 ]]; do
  case $1 in
    --allow-undefined) allow_undef=1 ;;
    --roots) roots=$2; shift ;;
    --cflags) user_cflags=$2; shift ;;
    --extra) extras+=("$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"); shift ;;
    --exclude) excludes+=("$2"); shift ;;
    --out) case $2 in /*) out=$2 ;; *) out=$PWD/$2 ;; esac; shift ;;
    --name) name=$2; shift ;;
    -j) jobs=$2; shift ;;
    -j*) jobs=${1#-j} ;;
    --no-opt) do_opt=0 ;;
    --quiet) quiet=1 ;;
    -h|--help) awk 'NR>1 && /^#/ {sub(/^# ?/, ""); print; next} NR>1 {exit}' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
  shift
done
[[ -n $name ]] || name=$(basename "$out" .wasm)
log() { [[ $quiet == 1 ]] || echo "$@"; }

outdir=$(dirname "$out")
objdir="$outdir/obj-$(basename "$out" .wasm)"
mkdir -p "$outdir"
rm -rf "$objdir"
mkdir -p "$objdir"

# ---- collect sources ----
srcs=()
for r in $roots; do
  [[ -d $here/$r ]] || continue
  while IFS= read -r f; do
    rel=${f#$here/}
    skip=0
    for ex in "${excludes[@]+"${excludes[@]}"}"; do
      # shellcheck disable=SC2053
      [[ $rel == $ex ]] && skip=1
    done
    [[ $skip == 1 ]] || srcs+=("$f")
  done < <(find "$here/$r" -name '*.c' -type f | sort)
done
for e in "${extras[@]+"${extras[@]}"}"; do srcs+=("$e"); done
if [[ ${#srcs[@]} == 0 ]]; then echo "no sources found" >&2; exit 1; fi
log "compiling ${#srcs[@]} files (-j$jobs)..."

# ---- compile (parallel), one bitcode object per source ----
compile_one() {
  local src=$1 here=$2 objdir=$3 CC=$4 user_cflags=$5
  local rel=${src#$here/}
  [[ $rel == /* ]] && rel=extra/$(basename "$src")
  local obj="$objdir/${rel%.c}.o"
  mkdir -p "$(dirname "$obj")"
  local dir; dir=$(dirname "$src")
  local extra=""
  local d=$dir
  while [[ $d == "$here"* && $d != "$here" ]]; do
    [[ -f $d/cflags.txt ]] && extra="$(cat "$d/cflags.txt") $extra"
    d=$(dirname "$d")
  done
  # an --extra file outside the napkin tree only picks up a cflags.txt next to it
  [[ $dir != "$here"* && -f $dir/cflags.txt ]] && extra="$(cat "$dir/cflags.txt")"
  # shellcheck disable=SC2086
  if "$CC" --target=wasm32-wasip1 -Oz -flto -std=gnu11 -DNDEBUG -c -o "$obj" \
      -I "$here/rt" -I "$here/src" -I "$dir" $user_cflags $extra \
      -w -Werror=implicit-function-declaration -Werror=int-conversion \
      -Werror=incompatible-pointer-types -Werror=return-type \
      "$src" 2> "$obj.log"; then
    rm -f "$obj.log"
  else
    rm -f "$obj"
    echo "$rel" >> "$objdir/FAILED"
  fi
}
export -f compile_one
printf '%s\0' "${srcs[@]}" | xargs -0 -P "$jobs" -I{} bash -c 'compile_one "$1" "$2" "$3" "$4" "$5"' _ {} "$here" "$objdir" "$CC" "$user_cflags"

nfail=0
if [[ -f $objdir/FAILED ]]; then
  sort -o "$objdir/FAILED" "$objdir/FAILED"
  nfail=$(wc -l < "$objdir/FAILED")
  echo "== $nfail file(s) failed to compile (skipped):"
  while IFS= read -r rel; do
    echo "  FAIL $rel"
    lf="$objdir/${rel%.c}.o.log"
    [[ $rel == extra/* ]] || true
    [[ -f $lf ]] && grep -m3 'error:' "$lf" | sed 's/^/      /'
  done < "$objdir/FAILED"
fi
mapfile -t objs < <(find "$objdir" -name '*.o' | sort)
log "compiled ${#objs[@]} objects, $nfail failed"
[[ ${#objs[@]} -gt 0 ]] || exit 1

# ---- link ----
ldflags=(-mexec-model=reactor -Wl,--gc-sections -Wl,--strip-debug -Wl,--error-limit=0)
[[ $allow_undef == 1 ]] && ldflags+=(-Wl,--allow-undefined)
pre="${out%.wasm}.pre-opt.wasm"
linklog="$outdir/$(basename "$out" .wasm).link.log"
if ! "$CC" --target=wasm32-wasip1 -Oz -flto "${ldflags[@]}" -o "$pre" "${objs[@]}" > "$linklog" 2>&1; then
  undef="$outdir/undefined.txt"
  grep -o 'undefined symbol: .*' "$linklog" | sed 's/undefined symbol: //' | sort -u > "$undef"
  nu=$(wc -l < "$undef")
  if [[ $nu -gt 0 ]]; then
    echo "== link failed: $nu undefined symbol(s) (full list: ${undef#$here/}); rerun with --allow-undefined to import them:"
    head -60 "$undef" | sed 's/^/  /'
    [[ $nu -gt 60 ]] && echo "  ... ($((nu - 60)) more)"
  else
    echo "== link failed:"; head -40 "$linklog"
  fi
  exit 1
fi
if [[ $allow_undef == 1 ]]; then
  # report what became an import because it was undefined (env imports not declared with JZ_IMPORT)
  python3 -I - "$pre" <<'PY' > "$outdir/imports.txt" || true
import sys
d = open(sys.argv[1], 'rb').read()
def leb(i):
    r = s = 0
    while True:
        b = d[i]; i += 1; r |= (b & 0x7f) << s; s += 7
        if b < 0x80: return r, i
i = 8
while i < len(d):
    sid = d[i]; i += 1
    size, i = leb(i)
    end = i + size
    if sid == 2:
        n, i = leb(i)
        for _ in range(n):
            ml, i = leb(i); mod = d[i:i+ml].decode(); i += ml
            nl, i = leb(i); nm = d[i:i+nl].decode(); i += nl
            kind = d[i]; i += 1
            if kind == 0: _, i = leb(i)
            elif kind == 1: i += 1; fl = d[i]; i += 1; _, i = leb(i); i = leb(i)[1] if fl & 1 else i
            elif kind == 2: fl = d[i]; i += 1; _, i = leb(i); i = leb(i)[1] if fl & 1 else i
            elif kind == 3: i += 2
            print(f"{mod}.{nm}")
    i = end
PY
  log "module imports: $(wc -l < "$outdir/imports.txt") (wasi + JZ_IMPORT host functions + symbols left undefined; list: ${outdir#$here/}/imports.txt)"
fi

if [[ $do_opt == 1 ]]; then
  if ! "$WASM_OPT" -O -g --low-memory-unused "$pre" -o "$out" 2> "$outdir/wasm-opt.log"; then
    echo "wasm-opt failed:"; head -20 "$outdir/wasm-opt.log"; cp "$pre" "$out"
  fi
else
  cp "$pre" "$out"
fi

# ---- sizes ----
sizes() {
  python3 -I -c '
import sys, gzip, brotli
d = open(sys.argv[1], "rb").read()
print(f"{len(d):>9}  {len(gzip.compress(d, 9)):>9}  {len(brotli.compress(d, quality=11)):>9}")' "$1"
}
printf '%-28s %9s  %9s  %9s\n' "$name" raw gzip-9 brotli-11
printf '%-28s %s\n' "  linked (pre wasm-opt)" "$(sizes "$pre")"
[[ $do_opt == 1 ]] && printf '%-28s %s\n' "  wasm-opt -O" "$(sizes "$out")"
log "wrote ${out#$here/}"
exit 0
