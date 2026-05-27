import { describe, expect, it } from "vitest";

describe("dev catalogue API exports", () => {
  it("exports project-level catalogue operations from jazz-tools/dev", async () => {
    const dev = await import("./index.js");

    expect(typeof dev.pushSchema).toBe("function");
    expect(typeof dev.pushPermissions).toBe("function");
    expect(typeof dev.pushMigration).toBe("function");
    expect(typeof dev.deploy).toBe("function");
    expect(typeof dev.pushSchemaCatalogue).toBe("function");
  });

  it("keeps pushSchemaCatalogue compatible across dev and testing entrypoints", async () => {
    const dev = await import("./index.js");
    const testing = await import("../testing/index.js");

    expect(testing.pushSchemaCatalogue).toBe(dev.pushSchemaCatalogue);
  });
});
