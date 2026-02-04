---
name: benchmarking
description: Use this skill when writing or running performance benchmarks for Jazz packages. Covers cronometro setup, file conventions, gotchas with worker threads, and how to compare implementations.
---

# Writing Benchmarks

## When to Use This Skill

* **Comparing implementations:** Measuring old vs new approach after an optimization
* **Regression testing:** Verifying a refactor doesn't degrade performance
* **Comparing with published version:** Benchmarking workspace code against the latest published npm package

## Do NOT Use This Skill For

* General app-level performance optimization (use `jazz-performance`)
* Profiling or debugging slow user-facing behavior

## Directory Structure

All benchmarks live in the `bench/` directory at the repository root:

```
bench/
├── package.json              # Dependencies: cronometro, cojson, jazz-tools, vitest
├── jazz-tools/               # jazz-tools benchmarks
│   └── *.bench.ts
```

## File Naming

Benchmark files follow the pattern: `<subject>.<package>.bench.ts`

Examples:
- `comap.create.jazz-tools.bench.ts`
- `asBase64.jazz-tools.bench.ts`

## Benchmark Library: cronometro

Benchmarks use [cronometro](https://github.com/ShogunPanda/cronometro), which runs each test in an isolated **worker thread** for accurate measurement.

### Basic Template

```ts
import cronometro from "cronometro";

let data: SomeType;

await cronometro(
  {
    "descriptive test name": {
      async before() {
        // Setup — runs once before the test iterations
        data = prepareTestData();
      },
      test() {
        // The code being benchmarked — runs many times
        doWork(data);
      },
      async after() {
        // Cleanup — runs once after all iterations
        cleanup();
      },
    },
  },
  {
    iterations: 20,
    warmup: true,
    print: {
      colors: true,
      compare: true,
    },
    onTestError: (testName: string, error: unknown) => {
      console.error(`\nError in test "${testName}":`);
      console.error(error);
    },
  },
);
```

### Comparing Two Implementations

The typical pattern is to define both old and new implementations as standalone functions, then benchmark them side by side:

```ts
import cronometro from "cronometro";

function oldImplementation(input: InputType): OutputType {
  // original code
}

function newImplementation(input: InputType): OutputType {
  // optimized code
}

let data: InputType;

await cronometro(
  {
    "old (description)": {
      async before() {
        data = generateInput();
      },
      test() {
        oldImplementation(data);
      },
    },
    "new (description)": {
      async before() {
        data = generateInput();
      },
      test() {
        newImplementation(data);
      },
    },
  },
  {
    iterations: 20,
    warmup: true,
    print: { colors: true, compare: true },
    onTestError: (testName: string, error: unknown) => {
      console.error(`\nError in test "${testName}":`);
      console.error(error);
    },
  },
);
```

### Comparing workspace vs published package

To compare current workspace code against the latest published version:

**1. Add npm aliases to `bench/package.json`:**

```json
{
  "dependencies": {
    "cojson": "workspace:*",
    "cojson-latest": "npm:cojson@0.20.7",
    "jazz-tools": "workspace:*",
    "jazz-tools-latest": "npm:jazz-tools@0.20.7"
  }
}
```

Then run `pnpm install` in `bench/`.

**2. Import both versions:**

```ts
import * as localTools from "jazz-tools";
import * as latestPublishedTools from "jazz-tools-latest";
import { WasmCrypto as LocalWasmCrypto } from "cojson/crypto/WasmCrypto";
import { WasmCrypto as LatestPublishedWasmCrypto } from "cojson-latest/crypto/WasmCrypto";
```

**3. Use `@ts-expect-error` when passing the published package** since the types won't match the workspace version:

```ts
ctx = await createContext(
  // @ts-expect-error version mismatch
  latestPublishedTools,
  LatestPublishedWasmCrypto,
);
```

### Benchmarking with a Jazz context

When benchmarking CoValues (not standalone functions), create a full Jazz context. Use this helper pattern:

```ts
async function createContext(tools: typeof localTools, wasmCrypto: typeof LocalWasmCrypto) {
  const ctx = await tools.createJazzContextForNewAccount({
    creationProps: { name: "Bench Account" },
    peers: [],
    crypto: await wasmCrypto.create(),
    sessionProvider: new tools.MockSessionProvider(),
  });
  return { account: ctx.account, node: ctx.node };
}
```

Key points:
- Pass `peers: []` — benchmarks don't need network sync
- Use `MockSessionProvider` — avoids real session persistence
- Call `(ctx.node as any).gracefulShutdown()` in `after()` to clean up

### Multiple cronometro runs in one file

You can `await` multiple `cronometro()` calls sequentially to separate different benchmark categories (e.g. write vs read):

```ts
console.log("\n=== Write Benchmark ===\n");
await cronometro({ /* write tests */ }, options);

console.log("\n=== Read Benchmark ===\n");
await cronometro({ /* read tests */ }, options);
```

This is useful when:
- Write tests create data inside `test()` (measures creation)
- Read tests create data in `before()` and only measure reads in `test()`

### Test data strategy

**Pre-generate at module level** when the data itself isn't what you're measuring:

```ts
const chunks100k = makeChunks(100 * 1024, CHUNK_SIZE);
const chunks1m = makeChunks(1024 * 1024, CHUNK_SIZE);
const chunks5m = makeChunks(5 * 1024 * 1024, CHUNK_SIZE);
```

Note: cronometro workers re-import the file, so module-level data is regenerated per worker. This is fine — it just adds a small startup cost, not measurement noise.

**Choose chunk sizes to stress the right thing.** Small chunks (e.g. 4KB) create many transactions, exposing per-transaction overhead. Multiple total sizes (100KB, 1MB, 5MB) show how performance scales.

**Skip sizes that are too fast to measure meaningfully** — e.g. reading 100KB might complete so fast that measurement noise dominates.

## Running Benchmarks

Add a script entry to `bench/package.json`:

```json
{
  "scripts": {
    "bench:mytest": "node --experimental-strip-types --no-warnings ./jazz-tools/mytest.jazz-tools.bench.ts"
  }
}
```

Then run from the `bench/` directory:

```sh
cd bench
pnpm run bench:mytest
```

## Critical Gotchas

### 1. Use `node --experimental-strip-types`, NOT `tsx`

Cronometro spawns **worker threads** that re-import the benchmark file. Workers don't inherit tsx's custom ESM loader, so the TypeScript import fails silently and the benchmark hangs forever.

Use `node --experimental-strip-types --no-warnings` instead:

```json
"bench:foo": "node --experimental-strip-types --no-warnings ./jazz-tools/foo.bench.ts"
```

### 2. `before`/`after` hooks MUST be `async` or accept a callback

Cronometro's lifecycle hooks expect either:
- An **async function** (returns a Promise)
- A function that **accepts and calls a callback** parameter

A plain synchronous function that does neither will silently prevent the test from ever starting, causing the benchmark to hang indefinitely:

```ts
// BAD — test never starts, benchmark hangs
{
  before() {
    data = generateInput();  // sync, no callback, no promise
  },
  test() { ... },
}

// GOOD — async function returns a Promise
{
  async before() {
    data = generateInput();
  },
  test() { ... },
}

// ALSO GOOD — callback style
{
  before(cb: () => void) {
    data = generateInput();
    cb();
  },
  test() { ... },
}
```

### 3. `test()` can be sync or async

Unlike `before`/`after`, the `test` function works correctly as a plain synchronous function. Make it `async` only if the code under test is genuinely asynchronous.

### 4. TypeScript constraints under `--experimental-strip-types`

Node's type stripping handles annotations, `as` casts, and `!` assertions. But it does **not** support:
- `enum` declarations (use `const` objects instead)
- `namespace` declarations
- Parameter properties in constructors (`constructor(private x: number)`)
- Legacy `import =` / `export =` syntax

Keep benchmark files to simple TypeScript that only uses type annotations, interfaces, type aliases, and casts.

## Example: Full Benchmark

```ts
import cronometro from "cronometro";

function makeTestData(size: number) {
  const chunks: Uint8Array[] = [];
  let remaining = size;
  while (remaining > 0) {
    const chunkSize = Math.min(4096, remaining);
    const chunk = new Uint8Array(chunkSize);
    for (let i = 0; i < chunkSize; i++) {
      chunk[i] = Math.floor(Math.random() * 256);
    }
    chunks.push(chunk);
    remaining -= chunkSize;
  }
  return { chunks };
}

function oldApproach(data: { chunks: Uint8Array[] }): string {
  let binary = "";
  for (const chunk of data.chunks) {
    for (let i = 0; i < chunk.length; i++) {
      binary += String.fromCharCode(chunk[i]!);
    }
  }
  return btoa(binary);
}

function newApproach(data: { chunks: Uint8Array[] }): string {
  let totalLen = 0;
  for (const chunk of data.chunks) totalLen += chunk.length;
  const merged = new Uint8Array(totalLen);
  let offset = 0;
  for (const chunk of data.chunks) {
    merged.set(chunk, offset);
    offset += chunk.length;
  }
  const CHUNK = 32768;
  const parts: string[] = [];
  for (let i = 0; i < totalLen; i += CHUNK) {
    parts.push(
      String.fromCharCode.apply(
        null,
        merged.subarray(i, Math.min(i + CHUNK, totalLen)) as unknown as number[],
      ),
    );
  }
  return btoa(parts.join(""));
}

let data: { chunks: Uint8Array[] };

await cronometro(
  {
    "old - 1MB": {
      async before() { data = makeTestData(1024 * 1024); },
      test() { oldApproach(data); },
    },
    "new - 1MB": {
      async before() { data = makeTestData(1024 * 1024); },
      test() { newApproach(data); },
    },
  },
  {
    iterations: 20,
    warmup: true,
    print: { colors: true, compare: true },
    onTestError: (testName: string, error: unknown) => {
      console.error(`\nError in test "${testName}":`);
      console.error(error);
    },
  },
);
```

## Checklist

- [ ] Benchmark file placed in `bench/jazz-tools/` with `*.bench.ts` naming
- [ ] Script added to `bench/package.json` using `node --experimental-strip-types --no-warnings`
- [ ] `before`/`after` hooks are `async` (not plain sync)
- [ ] `iterations` set to at least 20 for stable results
- [ ] `warmup: true` enabled
- [ ] `onTestError` handler included to surface worker failures
- [ ] Test names are descriptive (include what's being compared and data size)
- [ ] When comparing vs published: npm aliases added to `bench/package.json` and `pnpm install` run
- [ ] When using Jazz context: `gracefulShutdown()` called in `after()` hook
- [ ] Test data pre-generated at module level (not inside `test()`) when data creation isn't what's being measured
- [ ] Multiple data sizes tested to show scaling behavior
