import { schema as s } from "../../../packages/jazz-tools/src/schema-namespace.ts";
import {
  NativeRuntimeAdapter,
  encodeSchema,
} from "../../../packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts";
import { openConfig } from "../../../packages/jazz-tools/src/runtime/native-runtime/native-codec.ts";
import { translateQuery } from "../../../packages/jazz-tools/src/runtime/query-adapter.ts";
import { IndexedDbPageStore } from "../../../packages/jazz-tools/src/runtime/indexeddb-page-store.ts";

const app = s.defineApp({
  tasks: s.table({ title: s.string(), done: s.boolean(), rank: s.int(), body: s.string() }, {}),
});
const author = new TextEncoder().encode(
  '["00000000-0000-4000-8000-000000000073","https://wasm-size.test","member"]',
);
const query = translateQuery(app.tasks._build(), app.wasmSchema);
const pageQuery = translateQuery(
  app.tasks.where({ done: false }).limit(100)._build(),
  app.wasmSchema,
);
const expectedId = (i: number) =>
  `00000000-0000-4000-8000-${(i + 1).toString(16).padStart(12, "0")}`;
function verify(rows: any, count: number, filtered = false) {
  const seen = new Set<number>();
  if (!Array.isArray(rows) || rows.length !== count)
    throw Error(`row count ${rows?.length} != ${count}`);
  for (const row of rows) {
    const n = Number.parseInt(row.id.slice(-12), 16) - 1;
    if (
      n < 0 ||
      n >= (filtered ? count * 2 : count) ||
      seen.has(n) ||
      row.id !== expectedId(n) ||
      row.values[0]?.value !== `Task ${n}` ||
      row.values[1]?.value !== (n % 2 !== 0) ||
      row.values[2]?.value !== n ||
      row.values[3]?.value !== "payload ".repeat(24)
    )
      throw Error(`wrong row ${JSON.stringify(row)}`);
    if (filtered && n % 2 !== 0) throw Error("predicate mismatch");
    seen.add(n);
  }
  return rows.map((row: any) => row.id).join(",");
}
export async function runBundleWorkload({
  artifact,
  rowCount = 2000,
}: {
  artifact: string;
  rowCount?: number;
}) {
  const phases: any = {};
  let start = performance.now();
  const wasm = await import(/* @vite-ignore */ `/${artifact}/jazz_wasm.js`);
  phases.js_import = performance.now() - start;
  const bytes = await (await fetch(`/${artifact}/jazz_wasm_bg.wasm`)).arrayBuffer();
  start = performance.now();
  const module = await WebAssembly.compile(bytes);
  phases.compile = performance.now() - start;
  start = performance.now();
  await wasm.default({ module_or_path: module });
  phases.instantiate = performance.now() - start;
  const scenarios: any[] = [];
  for (const storage of ["memory", "indexeddb"]) {
    const timing: any = {};
    const measured = async (name: string, fn: () => any) => {
      const t = performance.now();
      try {
        return await fn();
      } finally {
        timing[name] = performance.now() - t;
      }
    };
    const dbName = `wasm-size-${crypto.randomUUID()}`,
      owner = "wasm-size-owner";
    let store: any;
    let node = new Uint8Array(16);
    node[0] = 0x73;
    async function open() {
      if (storage === "indexeddb") {
        store = await IndexedDbPageStore.open(dbName, { owner });
        node = store.replicaNode;
      }
      const db =
        storage === "memory"
          ? wasm.WasmDb.openMemory(encodeSchema(app.wasmSchema), openConfig(node, author, 1, true))
          : await wasm.WasmDb.openBrowser(
              store,
              encodeSchema(app.wasmSchema),
              openConfig(node, author, 1, true),
              owner,
            );
      return NativeRuntimeAdapter.fromDb(db, app.wasmSchema, node, author, 1, true, {
        scopeIsolatedRelay: storage === "indexeddb",
      });
    }
    let runtime = await measured("open", open);
    try {
      await measured("seed", async () => {
        const tx = crypto.randomUUID().replaceAll("-", "");
        runtime.beginTransaction("mergeable", tx as any);
        const ctx = JSON.stringify({ transaction_id: tx });
        for (let i = 0; i < rowCount; i++)
          runtime.insert(
            "tasks",
            {
              title: { type: "Text", value: `Task ${i}` },
              done: { type: "Boolean", value: i % 2 !== 0 },
              rank: { type: "Integer", value: i },
              body: { type: "Text", value: "payload ".repeat(24) },
            },
            ctx,
            expectedId(i),
          );
        await runtime.waitForTransaction(runtime.commitTransaction(tx as any), "local");
      });
      const rows = await measured("first_all", () => runtime.query(query, null, "local"));
      const signature = verify(rows, rowCount);
      const page = await measured("first_page", () => runtime.query(pageQuery, null, "local"));
      verify(page, Math.min(100, Math.ceil(rowCount / 2)), true);
      await measured("repeat_all_10", async () => {
        for (let i = 0; i < 10; i++) verify(await runtime.query(query, null, "local"), rowCount);
      });
      if (storage === "indexeddb") {
        await measured("close", () => runtime.close());
        store.close();
        runtime = await measured("reopen", open);
        verify(await measured("reopen_all", () => runtime.query(query, null, "local")), rowCount);
      }
      scenarios.push({ storage, timing, rowCount, signature });
    } finally {
      await runtime.close();
      store?.close();
    }
  }
  return {
    phases,
    scenarios,
    wasm_bytes: bytes.byteLength,
    artifact_fingerprint: wasm.nativeArtifactFingerprint(),
  };
}
