import { NapiRuntime } from "jazz-napi";
import { schema as s } from "../../index.js";
import { serializeRuntimeSchema } from "../../drivers/schema-wire.js";
import { JazzClient, type Runtime } from "../client.js";

const app = s.defineApp({
  items: s.table({ text: s.string() }),
});
const runtime = NapiRuntime.inMemory(
  serializeRuntimeSchema(app.wasmSchema),
  "napi-callback-exit-test",
  "test",
  "main",
);
const client = JazzClient.connectWithRuntime(
  runtime as unknown as Runtime,
  {
    appId: "napi-callback-exit-test",
    schema: app.wasmSchema,
  },
  { onAuthFailure() {} },
);

client.subscribe(app.items, () => {});
await new Promise<void>((resolve) => setImmediate(resolve));
await client.shutdown();
console.log("runtime closed");
