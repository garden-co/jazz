import { beforeEach, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import type { Db } from "../runtime/db.js";
import type { SpaceRoot } from "./spaces.js";
import { encodeEnvelope, decodeEnvelope } from "./envelope.js";
import { encryptedRowSpaces } from "./encrypted-schema.js";
import { E2eeDataError } from "./data-error.js";
import { decryptCellRows, encryptCell } from "./cell-data.js";

const access = vi.hoisted(() => ({ crypto: vi.fn(), keys: vi.fn() }));
vi.mock("./lifecycle.js", () => ({ cellCryptoForDb: access.crypto, withSpaceKeys: access.keys }));

const uuid = (n: number) => `00000000-0000-4000-8000-${n.toString().padStart(12, "0")}`;
const app = s.defineApp({
  projects: s.table({ name: s.string() }, {}),
  notes: s
    .table(
      { projectId: s.uuid(), title: s.string(), bytes: s.bytes() },
      {
        project: s.rel("projects", "projectId"),
      },
    )
    .encrypted({ space: "projectId", columns: ["title", "bytes"] }),
});
const root = (identifier: number, epoch: number) =>
  ({
    id: uuid(identifier + 100),
    scopeId: uuid(90),
    identifier: uuid(identifier),
    epochId: uuid(epoch),
  }) as SpaceRoot;
const currentA = root(1, 12),
  oldA = root(1, 11),
  currentB = root(2, 22);
const secretFor = (epoch: SpaceRoot) => new Uint8Array([Number(epoch.epochId.slice(-2))]);
const mechanism = { id: "test.cell-batch", version: 1 };
const db = {
  authenticatedTableIdentity: vi.fn(async () => uuid(90)),
  tableIdentity: vi.fn(async () => uuid(90)),
  columnIdentity: vi.fn(async (_table, column: string) => uuid(column === "title" ? 91 : 92)),
} as unknown as Db;
let borrowed: Uint8Array[];
let plaintexts: Uint8Array[];
let failDecrypt: boolean;
let revokeDuringDecrypt: boolean;
let revoked: boolean;

beforeEach(() => {
  vi.clearAllMocks();
  borrowed = [];
  plaintexts = [];
  failDecrypt = false;
  revokeDuringDecrypt = false;
  revoked = false;
  access.crypto.mockResolvedValue({
    application: "test",
    cipher: {
      mechanism,
      async encrypt(secret: Uint8Array, _aad: Uint8Array, plaintext: Uint8Array) {
        const payload = new Uint8Array(1 + plaintext.length);
        payload[0] = secret[0]!;
        payload.set(plaintext, 1);
        return encodeEnvelope(mechanism, payload);
      },
      async decrypt(secret: Uint8Array, _aad: Uint8Array, ciphertext: Uint8Array) {
        if (failDecrypt) throw new Error("provider-private-diagnostic");
        const payload = decodeEnvelope(mechanism, ciphertext);
        if (payload[0] !== secret[0]) throw new Error("wrong epoch key");
        const plaintext = Buffer.from(payload.subarray(1));
        plaintexts.push(plaintext);
        if (revokeDuringDecrypt) revoked = true;
        return plaintext;
      },
    },
  });
  access.keys.mockImplementation(async (_db, _scope, identifier, use, history) => {
    if (revoked) throw new E2eeDataError("key-not-shared");
    for (const epoch of identifier === uuid(1)
      ? history
        ? [currentA, oldA]
        : [currentA]
      : [currentB]) {
      const secret = secretFor(epoch);
      borrowed.push(secret);
      try {
        await use(secret, epoch);
      } finally {
        secret.fill(0);
      }
    }
  });
});

async function stored(id: number, epoch: SpaceRoot, title: string) {
  const key = secretFor(epoch);
  return {
    id: uuid(id),
    projectId: epoch.identifier,
    title: await encryptCell(
      db,
      app.notes,
      uuid(id),
      "title",
      { type: "Text", value: title },
      key,
      epoch,
    ),
    bytes: await encryptCell(
      db,
      app.notes,
      uuid(id),
      "bytes",
      { type: "Bytea", value: new Uint8Array([id, 255]) },
      key,
      epoch,
    ),
  };
}

it("preserves interleaved row order, mixed epochs and projected identities without retaining keys", async () => {
  const rows = [
    await stored(31, oldA, "old"),
    await stored(32, currentB, "other"),
    await stored(33, currentA, "new"),
  ];
  // A single row may contain cells last updated in different epochs.
  rows[0]!.bytes = await encryptCell(
    db,
    app.notes,
    uuid(31),
    "bytes",
    { type: "Bytea", value: new Uint8Array([31, 255]) },
    secretFor(currentA),
    currentA,
  );
  const projected = { id: rows[2]!.id, title: rows[2]!.title, bytes: rows[2]!.bytes };
  encryptedRowSpaces.set(projected, currentA.identifier);
  const input = [rows[0]!, rows[1]!, projected];
  const result = await decryptCellRows(db, app.wasmSchema, "notes", input);
  expect(result).toEqual([
    { id: uuid(31), projectId: uuid(1), title: "old", bytes: new Uint8Array([31, 255]) },
    { id: uuid(32), projectId: uuid(2), title: "other", bytes: new Uint8Array([32, 255]) },
    { id: uuid(33), title: "new", bytes: new Uint8Array([33, 255]) },
  ]);
  expect(access.keys.mock.calls.map((call) => call[2])).toEqual([uuid(1), uuid(2), uuid(1)]);
  expect(borrowed.every((key) => key.every((byte) => byte === 0))).toBe(true);
  expect(plaintexts.every((value) => value.every((byte) => byte === 0))).toBe(true);
  expect(input[0]!.title).toBeInstanceOf(Uint8Array);
  // A later read must observe revocation rather than reuse this result's keys.
  access.keys.mockRejectedValue(new E2eeDataError("key-not-shared"));
  await expect(decryptCellRows(db, app.wasmSchema, "notes", input)).rejects.toMatchObject({
    code: "key-not-shared",
  });
});

it("rejects a complete batch on a missing epoch or cipher failure and releases borrowed keys", async () => {
  const rows = [await stored(31, currentA, "first"), await stored(33, root(1, 99), "unavailable")];
  await expect(decryptCellRows(db, app.wasmSchema, "notes", rows)).rejects.toMatchObject({
    code: "invalid-ciphertext",
  });
  expect(borrowed.every((key) => key.every((byte) => byte === 0))).toBe(true);
  failDecrypt = true;
  await expect(decryptCellRows(db, app.wasmSchema, "notes", [rows[0]!])).rejects.toEqual(
    new E2eeDataError("invalid-ciphertext"),
  );
  expect(borrowed.every((key) => key.every((byte) => byte === 0))).toBe(true);
});

it("borrows transaction-owned initial keys and leaves plaintext projections independent of key access", async () => {
  const row = await stored(31, currentA, "initial");
  const secret = secretFor(currentA);
  const initial = new Map([
    [`${currentA.scopeId}:${currentA.identifier}`, { root: currentA, secret }],
  ]);
  const observed = vi.fn();
  expect(
    (await decryptCellRows(db, app.wasmSchema, "notes", [row], initial, observed))[0]!.title,
  ).toBe("initial");
  expect(observed).not.toHaveBeenCalled();
  expect(secret).toEqual(secretFor(currentA));
  expect(access.keys).not.toHaveBeenCalled();
  access.crypto.mockRejectedValue(new Error("unavailable key store"));
  const plain = [{ id: row.id, projectId: row.projectId }];
  expect(await decryptCellRows(db, app.wasmSchema, "notes", plain)).toBe(plain);
});

it.each([false, true])(
  "rechecks authority after decryption before exposing a grouped result (history observer: %s)",
  async (observe) => {
    const rows = [await stored(31, currentA, "first"), await stored(33, currentA, "second")];
    revokeDuringDecrypt = true;
    const observed = vi.fn();
    await expect(
      decryptCellRows(db, app.wasmSchema, "notes", rows, undefined, observe ? observed : undefined),
    ).rejects.toMatchObject({
      code: "key-not-shared",
    });
    expect(borrowed.every((key) => key.every((byte) => byte === 0))).toBe(true);
    expect(plaintexts.every((value) => value.every((byte) => byte === 0))).toBe(true);
    expect(observed).not.toHaveBeenCalled();
  },
);

it.each([1, 2])("reports complete validated history after key release (%i rows)", async (count) => {
  const rows = [await stored(31, currentA, "current"), await stored(33, oldA, "old")].slice(
    0,
    count,
  );
  const nextA = root(1, 13);
  const useKeys = access.keys.getMockImplementation()!;
  let reads = 0;
  access.keys.mockImplementation(async (...args) => {
    // A batch's final recheck sees a rotation that the initial decode did not.
    if (++reads === 2) {
      const secret = secretFor(nextA);
      borrowed.push(secret);
      try {
        await args[3](secret, nextA);
      } finally {
        secret.fill(0);
      }
    }
    await useKeys(...args);
  });
  const observed = vi.fn(() => {
    expect(reads).toBe(count);
    expect(borrowed.every((key) => key.every((byte) => byte === 0))).toBe(true);
  });
  const result = await decryptCellRows(db, app.wasmSchema, "notes", rows, undefined, observed);
  expect(result.map((row) => row.title)).toEqual(count === 1 ? ["current"] : ["current", "old"]);
  expect(observed).toHaveBeenCalledExactlyOnceWith(
    currentA.identifier,
    new Set(
      count === 1
        ? [currentA.epochId, oldA.epochId]
        : [nextA.epochId, currentA.epochId, oldA.epochId],
    ),
  );
  expect(plaintexts.every((bytes) => bytes.every((byte) => byte === 0))).toBe(true);
});
