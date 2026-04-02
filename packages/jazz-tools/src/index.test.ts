import { describe, expect, it } from "vitest";
import * as jazzTools from "./index.js";

describe("public schema namespace exports", () => {
  it("exposes schema helpers only through the schema namespace", () => {
    expect(jazzTools.schema).toBeDefined();
    expect(typeof jazzTools.schema.table).toBe("function");
    expect(typeof jazzTools.schema.defineApp).toBe("function");
    expect(typeof jazzTools.schema.defineMigration).toBe("function");
    expect(typeof jazzTools.schema.definePermissions).toBe("function");
    expect(typeof jazzTools.schema.migrate).toBe("function");

    expect("table" in jazzTools).toBe(false);
    expect("col" in jazzTools).toBe(false);
    expect("defineSchema" in jazzTools).toBe(false);
    expect("defineApp" in jazzTools).toBe(false);
    expect("defineMigration" in jazzTools).toBe(false);
    expect("definePermissions" in jazzTools).toBe(false);
    expect("migrate" in jazzTools).toBe(false);
    expect("getCollectedSchema" in jazzTools).toBe(false);
    expect("getCollectedMigration" in jazzTools).toBe(false);
    expect("resetCollectedState" in jazzTools).toBe(false);
  });
});
