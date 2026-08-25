import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import type { NativeRowDelta } from "../drivers/types.js";
import type { Runtime } from "./client.js";
import { translateQuery } from "./query-adapter.js";
import {
  createNapiNativeRuntimeAdapter,
  hasJazzNapiBuild,
} from "./testing/napi-runtime-test-utils.js";
import { createWasmRuntime, hasJazzWasmBuild } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({
  projects: s.table({ name: s.string() }),
  todos: s.table({
    title: s.string(),
    projectId: s.ref("projects"),
  }),
});

async function terminalDeltaForProjectedInclude(
  runtime: Runtime,
  label: string,
): Promise<NativeRowDelta> {
  const deltas: NativeRowDelta[] = [];
  const handle = runtime.createSubscription(
    translateQuery(app.todos.select("title").include({ project: true })._build(), app.wasmSchema),
    null,
    "local",
  );
  runtime.executeSubscription(handle, (delta: NativeRowDelta) => deltas.push(delta));

  const project = runtime.insert("projects", {
    name: { type: "Text", value: `${label} project` },
  });
  runtime.insert("todos", {
    title: { type: "Text", value: `${label} todo` },
    projectId: { type: "Uuid", value: project.id },
  });

  for (let attempt = 0; attempt < 20; attempt += 1) {
    const terminalDelta = deltas.find((delta) => (delta.terminalOperations?.length ?? 0) > 0);
    if (terminalDelta) {
      runtime.unsubscribe(handle);
      return terminalDelta;
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  runtime.unsubscribe(handle);
  throw new Error(`${label}: projected include did not emit a terminal operation`);
}

async function expectProjectedIncludeTerminalLayout(
  runtime: Runtime,
  label: string,
): Promise<void> {
  const delta = await terminalDeltaForProjectedInclude(runtime, label);
  const layout = delta.terminalLayouts?.[0];

  expect(layout, `${label}: terminal operation must publish its layout`).toBeDefined();
  expect(layout?.publicFields).toEqual(
    expect.arrayContaining([
      {
        // The selected root still occupies its CurrentRow codec descriptor
        // field, while the terminal maps it to the public schema field.
        name: "title",
        descriptorFieldName: "user_title",
        slot: 1,
        carrier: "Logical",
      },
      {
        // The query encoder preserves the public include path in the terminal
        // descriptor, so this needs no late prefix recovery.
        name: "project",
        descriptorFieldName: "project",
        slot: 2,
        carrier: "Logical",
      },
    ]),
  );
}

async function expectProvenancePredicateUsesCoreMilliseconds(
  runtime: Runtime,
  label: string,
): Promise<void> {
  const updatedAtMs = 1_777_777_777_777;
  const project = runtime.insert("projects", {
    name: { type: "Text", value: `${label} provenance project` },
  });
  runtime.insert(
    "todos",
    {
      title: { type: "Text", value: `${label} provenance todo` },
      projectId: { type: "Uuid", value: project.id },
    },
    // Native runtime mutation contexts and CurrentRow provenance use Unix ms.
    JSON.stringify({ updated_at: updatedAtMs }),
  );

  // The hop forces the relation-IR path shared by NAPI and WASM. That IR is
  // evaluated against the physical-ms CurrentRow, so the user-facing Date
  // must match without a second provenance conversion.
  const query = translateQuery(
    app.todos
      .where({ $updatedAt: { gte: new Date(updatedAtMs) } })
      .hopTo("project")
      ._build(),
    app.wasmSchema,
  );
  const rows = await runtime.query(query, null, "local");
  expect(rows, `${label}: provenance predicate must retain the matching row`).toHaveLength(1);
}

describe.skipIf(!hasJazzNapiBuild())("NAPI projected terminal layout", () => {
  it("binds selected root and include names before emitting terminal operations", async () => {
    await expectProjectedIncludeTerminalLayout(
      await createNapiNativeRuntimeAdapter(app.wasmSchema, {
        appId: "terminal-layout-napi-regression",
      }),
      "NAPI",
    );
  });

  it("evaluates relation provenance predicates in core physical milliseconds", async () => {
    await expectProvenancePredicateUsesCoreMilliseconds(
      await createNapiNativeRuntimeAdapter(app.wasmSchema, {
        appId: "provenance-predicate-napi-regression",
      }),
      "NAPI",
    );
  });
});

describe.skipIf(!hasJazzWasmBuild())("WASM projected terminal layout", () => {
  it("binds selected root and include names before emitting terminal operations", async () => {
    await expectProjectedIncludeTerminalLayout(
      await createWasmRuntime(app.wasmSchema, { appId: "terminal-layout-wasm-regression" }),
      "WASM",
    );
  });

  it("evaluates relation provenance predicates in core physical milliseconds", async () => {
    await expectProvenancePredicateUsesCoreMilliseconds(
      await createWasmRuntime(app.wasmSchema, {
        appId: "provenance-predicate-wasm-regression",
      }),
      "WASM",
    );
  });
});
