import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import type { StreamingValueSource } from "./client.js";
import {
  createNapiNativeRuntimeAdapter,
  hasJazzNapiBuild,
} from "./testing/napi-runtime-test-utils.js";
import { createWasmRuntime, hasJazzWasmBuild } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({ files: s.table({ data: s.bytes() }, {}) }, { migrations: "none" });

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

async function deadline<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("upload abort waited for source cleanup")),
          1_000,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

for (const host of ["NAPI", "WASM"] as const) {
  describe.skipIf(host === "NAPI" ? !hasJazzNapiBuild() : !hasJazzWasmBuild())(
    `${host} streaming source cleanup`,
    () => {
      for (const kind of ["readable stream", "async iterator"] as const) {
        it(`aborts a failed upload before ${kind} cleanup resolves`, async () => {
          const runtime =
            host === "NAPI"
              ? await createNapiNativeRuntimeAdapter(
                  app.wasmSchema,
                  {},
                  {
                    appId: `stream-source-${kind}`,
                  },
                )
              : await createWasmRuntime(app.wasmSchema, { appId: `stream-source-${kind}` });
          const started = deferred();
          const cleanup = deferred();
          const bytes = new Uint8Array(200_000).fill(7);
          let cleanupCalls = 0;
          const finishCleanup = () => {
            cleanupCalls += 1;
            started.resolve();
            return cleanup.promise;
          };
          const readable =
            kind === "readable stream"
              ? new ReadableStream<Uint8Array | string>({
                  start(controller) {
                    controller.enqueue(bytes);
                    controller.enqueue("invalid bytes");
                  },
                  cancel: finishCleanup,
                })
              : undefined;
          let position = 0;
          const source: StreamingValueSource = readable ?? {
            [Symbol.asyncIterator]() {
              return {
                async next() {
                  return {
                    done: false as const,
                    value: position++ === 0 ? bytes : "invalid bytes",
                  };
                },
                async return() {
                  await finishCleanup();
                  return { done: true as const, value: undefined };
                },
              };
            },
          };
          const rejected = runtime.streamingMutation!("insert", "files", {}, "data", source).then(
            () => {
              throw new Error("invalid byte stream was accepted");
            },
            (error: unknown) => error,
          );
          try {
            await deadline(started.promise);
            // Source cleanup is deliberately unresolved: the native abort and
            // admission release must not depend on application cleanup finishing.
            expect(await deadline(rejected)).toMatchObject({
              message: "Bytea streams require Uint8Array chunks",
            });
            expect(cleanupCalls).toBe(1);
            if (readable) expect(readable.locked).toBe(false);
            await expect(runtime.query(JSON.stringify({ table: "files" }))).resolves.toEqual([]);

            const successful = await runtime.streamingMutation!(
              "insert",
              "files",
              {},
              "data",
              (async function* () {
                yield new Uint8Array([1, 2, 3]);
              })(),
            );
            const rows = await runtime.query(JSON.stringify({ table: "files" }));
            expect(rows).toEqual([
              {
                id: successful.id,
                table: "files",
                values: [{ type: "Bytea", value: new Uint8Array([1, 2, 3]) }],
              },
            ]);
            await deadline(Promise.resolve(runtime.close?.()));
          } finally {
            // Also release the deliberately hung source on the unfixed base,
            // so a regression produces a failed assertion rather than a leak.
            cleanup.resolve();
            await rejected;
          }
        });
      }
    },
  );
}
