import { execFile } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { JazzClient } from "./client.js";
import { NativeRuntimeAdapter } from "./native-runtime/native-runtime-adapter.js";
import { hasJazzNapiBuild, loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import { createWasmRuntime, hasJazzWasmBuild } from "./testing/wasm-runtime-test-utils.js";

const require = createRequire(import.meta.url);
const execFileAsync = promisify(execFile);
const fixture = process.env.JAZZ_LARGE_VALUE_CREATION_FIXTURE;
const testFile = fileURLToPath(import.meta.url);
const packageRoot = join(dirname(testFile), "../..");
const vitestCli = join(dirname(require.resolve("vitest/package.json")), "vitest.mjs");
// The direct-WASM-text fixture additionally verifies that a resident write can
// hand off to update/delete/restore before its first local settlement. That is
// three more asynchronous persistence turns after creation. Keep the watchdog
// finite enough to catch the historical spin, while allowing that real work to
// make progress when the parent test process is sharing CI CPU with the suite.
const CHILD_WATCHDOG_MS = 12_000;
const PARENT_TEST_TIMEOUT_MS = CHILD_WATCHDOG_MS + 3_000;

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
  blobs: {
    columns: [{ name: "data", column_type: { type: "Bytea" }, nullable: false }],
  },
  documents: {
    columns: [{ name: "body", column_type: { type: "Json" }, nullable: false }],
  },
};

const modes = [
  "direct-napi-text",
  "streaming-napi-text",
  "concurrent-streaming-napi-text",
  "concurrent-streaming-wasm-text",
  "direct-wasm-text",
  "streaming-wasm-text",
  "direct-wasm-bytes",
  "streaming-wasm-bytes",
  "direct-wasm-json",
  "streaming-wasm-json",
] as const;

describe.runIf(!fixture && hasJazzNapiBuild() && hasJazzWasmBuild())(
  "bounded large-value creation regressions",
  () => {
    it.each(modes)(
      "%s creation completes",
      async (mode) => {
        try {
          await execFileAsync(
            process.execPath,
            [vitestCli, "run", "--config", "vitest.config.ts", testFile],
            {
              cwd: packageRoot,
              encoding: "utf8",
              timeout: CHILD_WATCHDOG_MS,
              env: { ...process.env, JAZZ_LARGE_VALUE_CREATION_FIXTURE: mode },
            },
          );
        } catch (error) {
          const result = error as Error & { stderr?: string; stdout?: string; killed?: boolean };
          throw new Error(
            `${mode} large-value child failed${result.killed ? " (watchdog killed it)" : ""}\n${result.stderr ?? ""}${result.stdout ?? ""}`,
            { cause: error },
          );
        }
      },
      PARENT_TEST_TIMEOUT_MS,
    );
  },
);

describe.runIf(modes.includes(fixture as (typeof modes)[number]))(
  "large-value creation child fixture",
  () => {
    it(`runs ${fixture} creation`, async () => {
      const runtime = fixture?.includes("-wasm-")
        ? await createWasmRuntime(schema, { appId: `large-value-${fixture}` })
        : await createNapiRuntime();
      const client = JazzClient.connectWithRuntime(runtime, {
        appId: "large-value-creation-watchdog",
        schema,
      });
      try {
        console.error(`phase:${fixture}:start`);
        if (
          fixture === "concurrent-streaming-napi-text" ||
          fixture === "concurrent-streaming-wasm-text"
        ) {
          client.insert("todos", {
            title: { type: "Text", value: "existing" },
            done: { type: "Boolean", value: false },
          });
          const writes = await Promise.all(
            ["first", "second"].map((title) =>
              client.insertStreaming(
                "todos",
                { done: { type: "Boolean", value: false } },
                "title",
                oneChunk(title),
              ),
            ),
          );
          expect(writes).toHaveLength(2);
          await Promise.all(writes.map((write) => write.wait({ tier: "local" })));
          console.error(`phase:${fixture}:local`);
          return;
        }
        const largeText = "x".repeat(256 * 1024 + 1);
        const largeJson = JSON.stringify({ selected: { answer: 42 }, padding: largeText });
        const write = fixture?.startsWith("direct-")
          ? fixture.endsWith("-bytes")
            ? client.insert("blobs", {
                data: { type: "Bytea", value: new TextEncoder().encode(largeText) },
              })
            : fixture.endsWith("-json")
              ? client.insert("documents", { body: { type: "Text", value: largeJson } })
              : client.insert("todos", {
                  title: { type: "Text", value: "x".repeat(256 * 1024 + 1) },
                  done: { type: "Boolean", value: false },
                })
          : fixture?.endsWith("-bytes")
            ? await client.insertStreaming(
                "blobs",
                {},
                "data",
                oneChunk(new TextEncoder().encode(largeText)),
              )
            : fixture?.endsWith("-json")
              ? await client.insertStreaming("documents", {}, "body", oneChunk(largeJson))
              : await client.insertStreaming(
                  "todos",
                  { done: { type: "Boolean", value: false } },
                  "title",
                  oneChunk(largeText),
                );
        expect(write, `fixture ${fixture} must create a write`).toBeDefined();
        const residentWrite = write!;
        console.error(`phase:${fixture}:returned`);
        if (fixture === "direct-wasm-text") {
          console.error(`phase:${fixture}:resident-handoff:start`);
          expect(() =>
            client.insert(
              "todos",
              {
                title: { type: "Text", value: largeText },
                done: { type: "Boolean", value: false },
              },
              { id: residentWrite.value.id },
            ),
          ).toThrow(/already exists/i);
          const updated = client.update("todos", residentWrite.value.id, {
            done: { type: "Boolean", value: true },
          });
          const deleted = client.delete("todos", residentWrite.value.id);
          const restored = client.restore("todos", residentWrite.value.id, {
            title: { type: "Text", value: largeText },
            done: { type: "Boolean", value: true },
          });
          await updated.wait({ tier: "local" });
          await deleted.wait({ tier: "local" });
          await restored.wait({ tier: "local" });
          console.error(`phase:${fixture}:resident-handoff:local`);
        }
        if (fixture === "direct-wasm-bytes") {
          await expect(
            runtime.readValueRange!("blobs", residentWrite.value.id, "data", 0, largeText.length),
          ).resolves.toEqual(new TextEncoder().encode(largeText));
        }
        if (fixture === "direct-wasm-json") {
          await expect(
            runtime.readJsonPointer!(
              "documents",
              residentWrite.value.id,
              "body",
              "/selected/answer",
            ),
          ).resolves.toBe(42);
        }
        await residentWrite.wait({ tier: "local" });
        console.error(`phase:${fixture}:local`);
        if (fixture?.endsWith("-json")) {
          await expect(
            runtime.readJsonPointer!(
              "documents",
              residentWrite.value.id,
              "body",
              "/selected/answer",
            ),
          ).resolves.toBe(42);
        }
      } finally {
        await runtime.close!();
      }
    });
  },
);

async function* oneChunk(chunk: string | Uint8Array) {
  yield chunk;
}

async function createNapiRuntime(): Promise<NativeRuntimeAdapter> {
  const { NapiDb } = await loadNapiModule();
  return new NativeRuntimeAdapter(
    { openMemory: (encodedSchema, config) => NapiDb.openMemory(encodedSchema, config) as never },
    schema,
    deterministicBytes(`large-value-${fixture}:node`),
    new TextEncoder().encode(JSON.stringify(["urn:jazz:test", `large-value-${fixture}:author`])),
    1,
    true,
    { readAuthorizationHost: "trusted-serving" },
  );
}

function deterministicBytes(seed: string): Uint8Array {
  let hash = 0x811c9dc5;
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  for (let round = 0; round < 4; round += 1) {
    for (let index = 0; index < seed.length; index += 1) {
      hash ^= seed.charCodeAt(index) + round;
      hash = Math.imul(hash, 0x01000193);
    }
    view.setUint32(round * 4, hash >>> 0, true);
  }
  return bytes;
}
