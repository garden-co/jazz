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

Benchmark files follow the pattern: `<subject>.<operation>.bench.ts`

Each file should focus on **a single benchmark** comparing multiple implementations (e.g., `@latest` vs `@workspace`).

Examples:
- `comap.create.jazz-tools.bench.ts` — benchmarks CoMap creation
- `filestream.getChunks.bench.ts` — benchmarks FileStream.getChunks()
- `filestream.asBase64.bench.ts` — benchmarks FileStream.asBase64()
- `binaryCoStream.write.bench.ts` — benchmarks binary stream writes

## Benchmark Library: cronometro

Benchmarks use [cronometro](https://github.com/ShogunPanda/cronometro), which runs each test in an isolated **worker thread** for accurate measurement.

### Basic Template

```ts
import cronometro from "cronometro";

const TOTAL_BYTES = 5 * 1024 * 1024;
let data: SomeType;

await cronometro(
  {
    "operation - @latest": {
      async before() {
        // Setup — runs once before the test iterations
        data = prepareTestData(TOTAL_BYTES);
      },
      test() {
        // The code being benchmarked — runs many times
        latestImplementation(data);
      },
      async after() {
        // Cleanup — runs once after all iterations
        cleanup();
      },
    },
    "operation - @workspace": {
      async before() {
        data = prepareTestData(TOTAL_BYTES);
      },
      test() {
        workspaceImplementation(data);
      },
      async after() {
        cleanup();
      },
    },
  },
  {
    iterations: 50,
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

### Single Cronometro Instance Per Benchmark

Each benchmark file should have **a single `cronometro()` call** that compares multiple implementations of the same operation. This makes results easier to read and compare:

```ts
import cronometro from "cronometro";

const TOTAL_BYTES = 5 * 1024 * 1024;
let data: InputType;

await cronometro(
  {
    "operationName - @latest": {
      async before() {
        data = generateInput(TOTAL_BYTES);
      },
      test() {
        latestImplementation(data);
      },
      async after() {
        cleanup();
      },
    },
    "operationName - @workspace": {
      async before() {
        data = generateInput(TOTAL_BYTES);
      },
      test() {
        workspaceImplementation(data);
      },
      async after() {
        cleanup();
      },
    },
  },
  {
    iterations: 50,
    warmup: true,
    print: { colors: true, compare: true },
    onTestError: (testName: string, error: unknown) => {
      console.error(`\nError in test "${testName}":`);
      console.error(error);
    },
  },
);
```

Key principles:
- **One file = one benchmark** (e.g., `getChunks`, `asBase64`, `write`)
- **One cronometro call** comparing `@latest` vs `@workspace` (or old vs new)
- **Fixed data size** at the top of the file (e.g., `const TOTAL_BYTES = 5 * 1024 * 1024`)
- **Descriptive test names** with format `"operation - @implementation"`

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

### Test data strategy

**Define a fixed data size constant at the top of the file**, then generate test data inside the `before` hook:

```ts
const TOTAL_BYTES = 5 * 1024 * 1024; // 5MB

let chunks: Uint8Array[];

await cronometro({
  "operationName - @workspace": {
    async before() {
      chunks = makeChunks(TOTAL_BYTES, CHUNK_SIZE);
    },
    test() {
      doWork(chunks);
    },
  },
}, options);
```

**Choose a size large enough to measure meaningfully.** Small data (e.g., 100KB) may complete so fast that measurement noise dominates. 5MB is typically a good default for file/stream operations.

**All fixture generation must be done inside the `before` hook**, not at module level. This ensures data is created in the same worker thread that runs the test.

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

This example shows a benchmark comparing `getChunks()` between the published package and workspace code:

```ts
import cronometro from "cronometro";
import * as localTools from "jazz-tools";
import * as latestPublishedTools from "jazz-tools-latest";
import { WasmCrypto as LocalWasmCrypto } from "cojson/crypto/WasmCrypto";
import { cojsonInternals } from "cojson";
import { WasmCrypto as LatestPublishedWasmCrypto } from "cojson-latest/crypto/WasmCrypto";

const CHUNK_SIZE = cojsonInternals.TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE;
const TOTAL_BYTES = 5 * 1024 * 1024;

function makeChunks(totalBytes: number, chunkSize: number): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  let remaining = totalBytes;
  while (remaining > 0) {
    const size = Math.min(chunkSize, remaining);
    const chunk = new Uint8Array(size);
    for (let i = 0; i < size; i++) {
      chunk[i] = Math.floor(Math.random() * 256);
    }
    chunks.push(chunk);
    remaining -= size;
  }
  return chunks;
}

type Tools = typeof localTools;

async function createContext(tools: Tools, wasmCrypto: typeof LocalWasmCrypto) {
  const ctx = await tools.createJazzContextForNewAccount({
    creationProps: { name: "Bench Account" },
    peers: [],
    crypto: await wasmCrypto.create(),
    sessionProvider: new tools.MockSessionProvider(),
  });
  return { account: ctx.account, node: ctx.node, FileStream: tools.FileStream };
}

function populateStream(ctx: Awaited<ReturnType<typeof createContext>>, chunks: Uint8Array[]) {
  let totalBytes = 0;
  for (const c of chunks) totalBytes += c.length;
  const stream = ctx.FileStream.create({ owner: ctx.account });
  stream.start({ mimeType: "application/octet-stream", totalSizeBytes: totalBytes });
  for (const chunk of chunks) stream.push(chunk);
  stream.end();
  return stream;
}

const benchOptions = {
  iterations: 50,
  warmup: true,
  print: { colors: true, compare: true },
  onTestError: (testName: string, error: unknown) => {
    console.error(`\nError in test "${testName}":`);
    console.error(error);
  },
};

let readCtx: Awaited<ReturnType<typeof createContext>>;
let readStream: ReturnType<typeof populateStream>;

await cronometro(
  {
    "getChunks - @latest": {
      async before() {
        readCtx = await createContext(
          // @ts-expect-error version mismatch
          latestPublishedTools,
          LatestPublishedWasmCrypto,
        );
        readStream = populateStream(readCtx, makeChunks(TOTAL_BYTES, CHUNK_SIZE));
      },
      test() {
        readStream.getChunks();
      },
      async after() {
        (readCtx.node as any).gracefulShutdown();
      },
    },
    "getChunks - @workspace": {
      async before() {
        readCtx = await createContext(localTools, LocalWasmCrypto);
        readStream = populateStream(readCtx, makeChunks(TOTAL_BYTES, CHUNK_SIZE));
      },
      test() {
        readStream.getChunks();
      },
      async after() {
        (readCtx.node as any).gracefulShutdown();
      },
    },
  },
  benchOptions,
);
```

## Checklist

- [ ] One benchmark file per operation (e.g., `filestream.getChunks.bench.ts`)
- [ ] Single `cronometro()` call comparing `@latest` vs `@workspace`
- [ ] Fixed data size constant at top of file (e.g., `const TOTAL_BYTES = 5 * 1024 * 1024`)
- [ ] Benchmark file placed in `bench/jazz-tools/` with `*.bench.ts` naming
- [ ] Script added to `bench/package.json` using `node --experimental-strip-types --no-warnings`
- [ ] `before`/`after` hooks are `async` (not plain sync)
- [ ] `iterations` set to at least 50 for stable results
- [ ] `warmup: true` enabled
- [ ] `onTestError` handler included to surface worker failures
- [ ] Test names follow format `"operation - @implementation"` (e.g., `"getChunks - @workspace"`)
- [ ] When comparing vs published: npm aliases added to `bench/package.json` and `pnpm install` run
- [ ] When using Jazz context: `gracefulShutdown()` called in `after()` hook
- [ ] Test data generated inside `before()` hooks (not at module level or inside `test()`)
