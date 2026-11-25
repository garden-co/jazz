import { expect, it } from "vitest";
import type { WasmSchema } from "../../drivers/types.js";
import { EncryptionError } from "../../encryption/index.js";
import { CLIENT_WIRE_FEATURES } from "./websocket.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";

const schema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
} satisfies WasmSchema;

/** A native database that admits nothing: the test only waits for a key and closes. */
function idleDb(): Parameters<typeof NativeRuntimeAdapter.fromDb>[0] {
  return {
    wireFeatures: () => CLIENT_WIRE_FEATURES,
    setTickScheduler: () => undefined,
    onMutationError: () => undefined,
    tick: () => undefined,
    close: () => undefined,
  } as never;
}

it("ends a write waiting for its space's key when the database closes", async () => {
  const runtime = NativeRuntimeAdapter.fromDb(
    idleDb(),
    schema,
    new Uint8Array(16),
    new TextEncoder().encode('["urn:jazz:test","key-wait"]'),
    1,
    true,
  );
  let asked!: () => void;
  const askedForKey = new Promise<void>((resolve) => {
    asked = resolve;
  });
  runtime.attachEncryption(
    {
      encryptionSetOnline: () => undefined,
      encryptionSpaces: () => "{}",
      // The key's preparation never answers: the server is out of reach.
      encryptionCall: (op: string) => {
        if (op === "shutdown") return Promise.resolve("null");
        expect(op).toBe("retryWrite");
        asked();
        return new Promise<string>(() => {});
      },
    } as never,
    { stop: () => undefined },
    () => ({ stop: () => undefined }) as never,
  );
  const pending = Object.assign(new Error("waiting"), {
    reason: "key-pending",
    space: { scopeId: "scope", identifier: "project" },
  });
  const writing = Promise.resolve(
    runtime.retryAfterKey(() => {
      throw pending;
    }),
  ).catch((error: unknown) => error);
  await askedForKey;
  const closing = Date.now();
  await runtime.close();
  const error = await writing;
  expect(Date.now() - closing).toBeLessThan(1_000);
  expect(error).toBeInstanceOf(EncryptionError);
  expect((error as EncryptionError).code).toBe("closed");
});
