import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import type { Db } from "../runtime/db.js";
import { jazzAdapter } from "./index.js";
import { legacyDefinition, currentDefinition } from "./fixtures/issuer-upgrade-schema.js";

const legacyApp = s.defineApp(legacyDefinition, { migrations: "none" });
const currentApp = s.defineApp(currentDefinition, { migrations: "none" });

function fakeDb(accounts: Record<string, unknown>[]): Db {
  return { all: async () => accounts } as unknown as Db;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("Better Auth upgrade checks", () => {
  it("refuses a schema that still requires the account issuer", () => {
    expect(() => jazzAdapter({ db: () => fakeDb([]), schema: legacyApp.wasmSchema })({})).toThrow(
      /still requires "better_auth_account\.issuer".*#compatibility/s,
    );
  });

  it("accepts a schema without the issuer column", () => {
    expect(() =>
      jazzAdapter({ db: () => fakeDb([]), schema: currentApp.wasmSchema })({}),
    ).not.toThrow();
  });

  it("warns once, naming the rows, when account pairs are duplicated", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const adapter = jazzAdapter({
      db: () =>
        fakeDb([
          { id: "row-a", providerId: "github", accountId: "42" },
          { id: "row-b", providerId: "github", accountId: "42" },
          { id: "row-c", providerId: "github", accountId: "43" },
        ]),
      schema: currentApp.wasmSchema,
    })({});

    await adapter
      .update({
        model: "account",
        where: [{ field: "id", value: "none" }],
        update: {},
      })
      .catch(() => {});
    await adapter
      .update({
        model: "account",
        where: [{ field: "id", value: "none" }],
        update: {},
      })
      .catch(() => {});

    const duplicateWarnings = () =>
      warn.mock.calls.filter(([message]) => String(message).includes("more than one row"));
    await vi.waitFor(() => expect(duplicateWarnings()).toHaveLength(1));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(duplicateWarnings()).toHaveLength(1);
    expect(duplicateWarnings()[0]![0]).toContain('["github","42"]: rows row-a, row-b');
    expect(duplicateWarnings()[0]![0]).not.toContain("row-c");
  });

  it("stays quiet when every account pair is unique", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const adapter = jazzAdapter({
      db: () => fakeDb([{ id: "row-a", providerId: "github", accountId: "42" }]),
      schema: currentApp.wasmSchema,
    })({});

    await adapter
      .update({
        model: "account",
        where: [{ field: "id", value: "none" }],
        update: {},
      })
      .catch(() => {});
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(warn.mock.calls.some(([message]) => String(message).includes("more than one row"))).toBe(
      false,
    );
  });
});
