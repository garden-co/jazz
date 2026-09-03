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
      .default({ type: "ready", count: 7, label: "live", note: null }),
  },
});

export const app = { wasmSchema: generatedApp.wasmSchema };
