import { schema as s } from "../../../schema-namespace.js";
import { defineApp } from "../../../typed-app.js";

const generatedApp = defineApp({
  records: {
    state: s
      .enum({
        ready: {
          count: s.int().default(3),
          label: s.string().default("queued"),
          note: s.string().optional().default(null),
        },
      })
      .default({ type: "ready", count: 7, label: "live", note: "unused" }),
  },
});

const stateColumn = generatedApp.wasmSchema.records.columns[0]!;
if (stateColumn.default?.type !== "Enum") {
  throw new Error("Expected generated payload enum default.");
}
stateColumn.default.value.values[2] = { type: "Null" };

export const app = { wasmSchema: generatedApp.wasmSchema };
