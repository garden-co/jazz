import { blake3 } from "@noble/hashes/blake3.js";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import type { WasmRow } from "../drivers/types.js";
import type { Runtime } from "./client.js";
import { translateQuery } from "./query-adapter.js";
import fixture from "../../fixtures/union-arm-window-v1.json";
import {
  createNapiNativeRuntimeAdapter,
  hasJazzNapiBuild,
} from "./testing/napi-runtime-test-utils.js";
import { createWasmRuntime, hasJazzWasmBuild } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp(
  { rooms: s.table({ title: s.string(), code: s.string() }, {}) },
  { migrations: "none" },
);

it("pins windowed UNION labels while retaining the legacy unmodified label", () => {
  const seeds = [
    app.rooms,
    app.rooms.orderBy("title").offset(1).limit(1),
    app.rooms.orderBy("title").offset(2).limit(1),
    app.rooms.limit(0),
    app.rooms.offset(0),
  ];
  expect(fixture.encoding).toBe("jazz.sdk.union-arm-output.v1");
  for (const [index, seed] of seeds.entries()) {
    const expected = fixture.cases[index]!;
    const bytes = Buffer.from(expected.utf8_hex, "hex");
    expect(bytes.toString("utf8")).toBe(expected.preimage);
    expect(`derived:${Buffer.from(blake3(bytes)).toString("hex")}`).toBe(expected.label);
    expect(JSON.parse(app.union([seed])._build()).union.inputs[0].label).toBe(expected.label);
  }
});

it("keeps seed windows separate from subsequent UNION builder clones", () => {
  const seed = app.rooms.orderBy("title", "desc").offset(1).limit(2);
  const union = app.union({ page: seed });
  const changed = union.orderBy("title").offset(2).limit(1);
  const input = JSON.parse(changed._build()).union.inputs[0].input;
  expect(input).toMatchObject({ orderBy: [["title", "desc"]], offset: 1, limit: 2 });
  expect(JSON.parse(union._build())).toMatchObject({ orderBy: [] });
  expect(JSON.parse(union._build())).not.toHaveProperty("limit");
  expect(JSON.parse(changed._build())).toMatchObject({
    orderBy: [["title", "asc"]],
    offset: 2,
    limit: 1,
  });
  expect(JSON.parse(union._build()).union.inputs[0].label).toBe("page");
});

async function seed(runtime: Runtime) {
  for (const title of ["a", "b", "c", "d"]) {
    runtime.insert("rooms", {
      title: { type: "Text", value: title },
      code: { type: "Text", value: `code-${title}` },
    });
  }
}

async function titles(runtime: Runtime, query: { _build(): string }) {
  const rows: WasmRow[] = await runtime.query(translateQuery(query._build(), app.wasmSchema));
  return rows.map((row) => {
    // Check complete ordinary rows as well as the page's membership.
    expect(row.values).toHaveLength(2);
    const title = row.values[0]!;
    expect(title.type).toBe("Text");
    if (title.type !== "Text") throw new Error("expected a title");
    expect(row.values[1]).toEqual({ type: "Text", value: `code-${title.value}` });
    return title.value;
  });
}

for (const host of ["NAPI", "WASM"] as const) {
  describe.skipIf(host === "NAPI" ? !hasJazzNapiBuild() : !hasJazzWasmBuild())(
    `${host} UNION seed windows`,
    () => {
      async function open() {
        const runtime =
          host === "NAPI"
            ? await createNapiNativeRuntimeAdapter(
                app.wasmSchema,
                {},
                { appId: "union-seed-window" },
              )
            : await createWasmRuntime(app.wasmSchema, { appId: "union-seed-window" });
        await seed(runtime);
        return runtime;
      }

      it("keeps different pages of the same filter as distinct array arms", async () => {
        const runtime = await open();
        const query = app
          .union([
            app.rooms.orderBy("title").offset(1).limit(1),
            app.rooms.orderBy("title").offset(2).limit(1),
          ])
          .orderBy("title");
        expect(await titles(runtime, query)).toEqual(["b", "c"]);
      });

      it("applies each named arm's page before the outer UNION page", async () => {
        const runtime = await open();
        const query = app
          .union({
            recent: app.rooms.orderBy("title", "desc").offset(1).limit(1),
            first: app.rooms.orderBy("title").limit(1),
          })
          .orderBy("title", "desc");
        expect(await titles(runtime, query)).toEqual(["c", "a"]);
        expect(await titles(runtime, query.offset(1).limit(1))).toEqual(["a"]);
      });

      it("retains a zero limit and an offset without a limit", async () => {
        const runtime = await open();
        const query = app
          .union({
            empty: app.rooms.limit(0),
            tail: app.rooms.orderBy("title").offset(3),
          })
          .orderBy("title");
        expect(await titles(runtime, query)).toEqual(["d"]);
      });
    },
  );
}
