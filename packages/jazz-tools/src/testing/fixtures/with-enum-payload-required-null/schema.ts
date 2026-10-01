import { schema as s } from "../../../schema-namespace.js";
import { defineApp } from "../../../typed-app.js";

const generatedApp = defineApp({
  records: {
    state: s.enum({ ready: { label: s.string() } }).default({ type: "ready", label: "live" }),
  },
});

const stateColumn = generatedApp.wasmSchema.records.columns[0]!;
if (stateColumn.default?.type !== "Enum") {
  throw new Error("Expected generated payload enum default.");
}
stateColumn.default.value.values[0] = { type: "Null" };

export const app = { wasmSchema: generatedApp.wasmSchema };
