import { describe, expect, it } from "vitest";

import type { WasmSchema } from "../../drivers/types.js";
import { testApp } from "../../testing/test-app.js";
import { encodeSchema } from "./schema-codec.js";

describe("branch-aware schema codec", () => {
  it("preserves branchBy columns in the public schema source", () => {
    const schema: WasmSchema = {
      todos: {
        columns: [{ name: "workspace_id", column_type: { type: "Uuid" }, nullable: false }],
        branchBy: ["workspace_id"],
      },
    };

    const source = JSON.parse(new TextDecoder().decode(encodeSchema(schema)));
    expect(source).toEqual({ tables: schema });
  });

  it("exposes branchBy through the app's bound schema", () => {
    const app = testApp({
      tables: {
        branches: {
          columns: { name: "string" },
          relations: { todosViaBranch: { reverse: "todos", relation: "branch" } },
        },
        todos: {
          columns: { branch_id: "uuid", title: "string" },
          relations: { branch: { rel: "branches", column: "branch_id" } },
          branchBy: ["branch_id"],
        },
      },
    });

    expect(app.wasmSchema.todos?.branchBy).toEqual(["branch_id"]);
  });
});
