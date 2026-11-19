import { expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { plainSchema } from "./plain-schema.js";

it("hides declared equality tokens and keeps ordinary columns with the same prefix", () => {
  const stored: WasmSchema = {
    tasks: {
      columns: [
        { name: "title", column_type: { type: "Bytea" }, nullable: false },
        { name: "privateNote", column_type: { type: "Bytea" }, nullable: false },
        { name: "__e2ee_eq_title", column_type: { type: "Bytea" }, nullable: false },
        { name: "__e2ee_eq_privateNote", column_type: { type: "Text" }, nullable: false },
      ],
    },
  };
  const view = plainSchema(stored, {
    tasks: {
      columns: {
        title: { type: { columnType: { type: "Text" }, nullable: false } },
        privateNote: { type: { columnType: { type: "Text" }, nullable: true } },
      },
      equality: ["title"],
    },
  });

  expect(view.tasks!.columns).toEqual([
    { name: "title", column_type: { type: "Text" }, nullable: false },
    { name: "privateNote", column_type: { type: "Text" }, nullable: true },
    { name: "__e2ee_eq_privateNote", column_type: { type: "Text" }, nullable: false },
  ]);
  expect(plainSchema(stored)).toBe(stored);
  expect(stored.tasks!.columns).toHaveLength(4);
});
