import { col } from "../../../dsl.js";
import { defineApp } from "../../../typed-app.js";

const generatedApp = defineApp({
  records: {
    state: col
      .enum({
        ready: {
          count: col.int().default(3),
          label: col.string().default("queued"),
          note: col.string().optional().default(null),
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
