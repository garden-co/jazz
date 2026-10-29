import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { translateQuery } from "./query-adapter.js";
import {
  createNapiNativeRuntimeAdapter,
  hasJazzNapiBuild,
} from "./testing/napi-runtime-test-utils.js";
import { createWasmRuntime, hasJazzWasmBuild } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({
  nodes: s.table(
    { title: s.string(), parentId: s.uuid().optional() },
    { parent: s.rel("nodes", "parentId") },
  ),
});
const ids = [
  "00000000-0000-4000-8000-000000000001",
  "00000000-0000-4000-8000-000000000002",
  "00000000-0000-4000-8000-000000000003",
];
const gather = {
  maxDepth: 0,
  step: ({ current }: { current: string }) => app.nodes.where({ id: current }).hopTo("parent"),
};

for (const host of ["NAPI", "WASM"] as const) {
  describe.skipIf(host === "NAPI" ? !hasJazzNapiBuild() : !hasJazzWasmBuild())(
    `${host} gather output windows`,
    () => {
      async function open() {
        const runtime =
          host === "NAPI"
            ? await createNapiNativeRuntimeAdapter(
                app.wasmSchema,
                {},
                { appId: "gather-output-window" },
              )
            : await createWasmRuntime(app.wasmSchema, { appId: "gather-output-window" });
        for (const [index, title] of ["a", "b", "c"].entries()) {
          runtime.insert("nodes", { title: { type: "Text", value: title } }, null, ids[index]!);
          runtime.insert("nodes", {
            title: { type: "Text", value: `leaf-${title}` },
            parentId: { type: "Uuid", value: ids[index]! },
          });
        }
        return runtime;
      }

      it("keeps ordering and pagination on the gathered result when set before gather", async () => {
        const runtime = await open();
        try {
          const control = app.nodes
            .hopTo("parent")
            .gather(gather)
            .orderBy("title")
            .offset(1)
            .limit(1);
          const pagedBeforeGather = app.nodes
            .hopTo("parent")
            .orderBy("title")
            .offset(1)
            .limit(1)
            .gather(gather);
          const rows = await runtime.query(translateQuery(control._build(), app.wasmSchema));
          expect(rows).toHaveLength(1);
          expect(rows[0]!.values[0]).toEqual({ type: "Text", value: "b" });
          expect(
            await runtime.query(translateQuery(pagedBeforeGather._build(), app.wasmSchema)),
          ).toEqual(rows);
        } finally {
          await runtime.close?.();
        }
      });

      it("retains a zero output limit without placing it inside the gather seed", async () => {
        const runtime = await open();
        try {
          const empty = app.nodes.hopTo("parent").limit(0).gather(gather);
          expect(await runtime.query(translateQuery(empty._build(), app.wasmSchema))).toEqual([]);
        } finally {
          await runtime.close?.();
        }
      });
    },
  );
}
