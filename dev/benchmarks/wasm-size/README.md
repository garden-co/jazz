# Browser WASM size

Two tools for finding out what the shipped browser WASM is made of.

```sh
dev/benchmarks/wasm-size/build.sh target/wasm-size/out --type-names
python3 dev/benchmarks/wasm-size/attribute.py target/wasm-size/out/jazz_wasm_bg.wasm > report.md
```

`build.sh` reproduces the release build (`dev/artifacts/build.mjs`): fat LTO,
one codegen unit, then `wasm-opt -O -g --low-memory-unused`, then drops the mangling hashes from
function names as release builds do. It needs `wasm-bindgen` and
`wasm-opt` on `PATH`; `npm i binaryen` provides a `wasm-opt` that works
offline. A build takes about 20 minutes on four cores.

`attribute.py` reads the optimised module directly and reports code by kind
(own code, async fn bodies, B-tree, sort, drop glue, serde, dependencies), by
impl type or module, the workspace types that cause the most std generic code,
the largest functions, and near-duplicate functions that differ only in their
call targets.

Two rules keep the numbers honest:

- Measure the wasm-opt output. LLVM IR line counts (`cargo llvm-lines`) and
  pre-wasm-opt sizes overstate generic code several times over; they track
  compile time, not shipped size.
- Attribute with `--type-names` (v0 mangling, so names carry generic
  arguments), but compare sizes only between builds made the same way.
