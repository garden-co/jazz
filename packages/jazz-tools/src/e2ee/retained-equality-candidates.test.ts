import { beforeEach, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import type { Db } from "../runtime/db.js";
import type { SpaceRoot } from "./spaces.js";
import type { CellCipher } from "./types.js";
import { createBrowserCrypto } from "./browser.js";
import { encryptCell, decryptCellRows } from "./cell-data.js";
import { prepareEqualityQuery } from "./equality-query.js";

// Unit boundary only: real authenticated cells, no simulated admission proof.
const state = vi.hoisted(() => ({
  cipher: undefined as CellCipher | undefined,
  root: undefined as SpaceRoot | undefined,
  secret: new Uint8Array(32),
  valid: true,
  released: [] as Uint8Array[],
}));
vi.mock("./lifecycle.js", async (original) => ({
  ...(await original<typeof import("./lifecycle.js")>()),
  cellCryptoForDb: async () => ({ cipher: state.cipher!, application: "local-content-unit" }),
  contentIdentitiesForDb: async () => {
    if (!state.valid) throw new Error("retained owner invalidated");
    return { table: "notes-stable-id", column: "title-stable-id" };
  },
  withSpaceKeys: async () => {
    throw new Error("authority helper must not be used");
  },
  withContentSpaceKeys: async (
    _db: unknown,
    _scope: unknown,
    _id: unknown,
    use: (key: Uint8Array, root: Readonly<SpaceRoot>) => Promise<void>,
  ) => {
    if (!state.valid) throw new Error("retained owner invalidated");
    const key = state.secret.slice();
    try {
      await use(key, state.root!);
      if (!state.valid) throw new Error("retained owner invalidated");
    } finally {
      key.fill(0);
      state.released.push(key);
    }
  },
}));
const app = s.defineApp(
  {
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  },
  { migrations: "none" },
);
const db = {} as Db;
const projectId = "00000000-0000-4000-8000-000000000001";
beforeEach(async () => {
  state.cipher = (await createBrowserCrypto()).cellCipher;
  state.root = {
    id: crypto.randomUUID(),
    scopeId: crypto.randomUUID(),
    identifier: projectId,
    epochId: crypto.randomUUID(),
  } as SpaceRoot;
  state.secret = crypto.getRandomValues(new Uint8Array(32));
  state.valid = true;
  state.released = [];
});
async function cell(title: string, root = state.root!, secret = state.secret) {
  const id = crypto.randomUUID();
  return {
    id,
    projectId,
    title: await encryptCell(
      db,
      app.notes,
      id,
      "title",
      { type: "Text", value: title },
      secret,
      root,
    ),
  };
}

it.each(["nonmatching", "beyond-page"])(
  "refuses an uncached epoch in a %s candidate before filtering/paging",
  async (mode) => {
    const query = app.notes.where({ projectId, title: "match" }).limit(1);
    const plan = await prepareEqualityQuery(db, query, query._build(), undefined, true);
    const unknown = { ...state.root!, epochId: crypto.randomUUID() };
    const rows = [
      await cell("match"),
      await cell(
        mode === "nonmatching" ? "other" : "match",
        unknown,
        crypto.getRandomValues(new Uint8Array(32)),
      ),
    ];
    plan!.checkCandidates(rows);
    await expect(decryptCellRows(db, app.notes._schema, "notes", rows)).rejects.toMatchObject({
      code: "invalid-ciphertext",
    });
    expect(state.released.length).toBeGreaterThan(0);
    expect(state.released.every((key) => key.every((byte) => byte === 0))).toBe(true);
    plan!.dispose();
  },
);
it("decrypts all candidates then filters/page-matches canonical plaintext", async () => {
  const query = app.notes.where({ projectId, title: "match" }).offset(1).limit(1);
  const plan = await prepareEqualityQuery(db, query, query._build(), undefined, true);
  const rows = [await cell("other"), await cell("match"), await cell("match")];
  plan!.checkCandidates(rows);
  const plaintext = await decryptCellRows(db, app.notes._schema, "notes", rows);
  expect(plan!.verify(plaintext)).toEqual([{ ...rows[2], title: "match" }]);
  plan!.dispose();
});
it("refuses owner invalidation during awaited decryption and releases the borrow", async () => {
  const rows = [await cell("match")];
  const cipher = state.cipher!;
  state.cipher = {
    ...cipher,
    async decrypt(...args) {
      const value = await cipher.decrypt(...args);
      state.valid = false;
      return value;
    },
  };
  await expect(decryptCellRows(db, app.notes._schema, "notes", rows)).rejects.toThrow(
    "invalidated",
  );
  expect(state.released.length).toBe(1);
  expect(state.released[0]!.every((byte) => byte === 0)).toBe(true);
});
