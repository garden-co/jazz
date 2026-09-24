import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createBrowserCrypto } from "./browser.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { groupSchema } from "./groups.js";

const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
const bytes = (value: string) =>
  Uint8Array.from(value.match(/../g)!.map((pair) => Number.parseInt(pair, 16)));
// Native row corpus: a single variable-width field has no record offsets.
// The 02 tag is the existing string/bytes carrier, not an E2EE encoding.
const packed = ["0273796e746865746963", "02dead"];
// JE2E, format 1, "test.corpus", adapter version 1, then one fixture byte.
const inner = "4a453245010b746573742e636f7270757300000001";
const outer = "4a45324501156a617a7a2e653265652e63656c6c2d7265636f726400000001";

// Independent reader for the documented u32be-length transcript format.
function fields(value: Uint8Array): Uint8Array[] {
  const result: Uint8Array[] = [];
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
  for (let offset = 0; offset < value.length; ) {
    expect(offset + 4).toBeLessThanOrEqual(value.length);
    const length = view.getUint32(offset, false);
    offset += 4;
    expect(offset + length).toBeLessThanOrEqual(value.length);
    result.push(value.slice(offset, offset + length));
    offset += length;
  }
  return result;
}

it.each(["wasm", "native"])(
  "pins complete encrypted cell records through public %s writes and reads",
  async (runtime) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
      notes: s
        .table(
          { projectId: s.uuid(), title: s.string(), payload: s.bytes() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["title", "payload"] }),
    });
    // This is the ordinary physical schema a non-E2EE client sees, not an
    // inspection of private runtime state or a call to the cell encoder.
    const physical = s.defineApp({
      ...deviceRequestSchema,
      ...groupSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
      notes: s.table(
        { projectId: s.uuid(), title: s.bytes(), payload: s.bytes() },
        { project: s.rel("projects", "projectId") },
      ),
    });
    const policies = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    });
    const permissions = { ...deviceRequestPermissions, ...policies };
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let raw: Awaited<ReturnType<typeof createDb>> | undefined;
    let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
    const encrypted: { aad: Uint8Array; plaintext: string }[] = [];
    const decrypted: { aad: Uint8Array; index: number }[] = [];
    let saved: string | null = null;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      const crypto = await createBrowserCrypto();
      const e2ee = {
        app,
        store: {
          async read() {
            return saved;
          },
          async update(transform: (value: string | null) => string) {
            saved = transform(saved);
          },
        },
        crypto: {
          ...crypto,
          // Deliberately not cryptography: the BYOC boundary supplies a fixed
          // envelope so common framing can be checked independently of the cipher.
          cellCipher: {
            mechanism: { id: "test.corpus", version: 1 },
            async encrypt(_key: Uint8Array, aad: Uint8Array, plaintext: Uint8Array) {
              const index = encrypted.length;
              encrypted.push({ aad: aad.slice(), plaintext: hex(plaintext) });
              return bytes(inner + index.toString(16).padStart(2, "0"));
            },
            async decrypt(_key: Uint8Array, aad: Uint8Array, ciphertext: Uint8Array) {
              const index = ciphertext[ciphertext.length - 1]!;
              expect(hex(ciphertext)).toBe(inner + index.toString(16).padStart(2, "0"));
              decrypted.push({ aad: aad.slice(), index });
              return bytes(packed[index]!);
            },
          },
        },
      };
      let account;
      if (runtime === "native") {
        owner = await createJazzSession({
          appId: server.appId,
          serverUrl: server.url,
          app,
          permissions,
          driver: { type: "memory" },
          initial: "local-first",
          e2ee,
        });
        db = owner.getSnapshot().client!.db;
        account = owner.getSnapshot().account!;
      } else {
        const config = await localAccountConfig(server.appId, server.url);
        account = config.account;
        db = await createDb({ ...config, e2ee });
      }
      raw = await createDb({
        appId: server.appId,
        serverUrl: server.url,
        account,
        driver: { type: "memory" },
      });
      const tx = db.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title: "Corpus" });
      const note = tx.insert(app.notes, {
        projectId: project.id,
        title: "synthetic",
        payload: new Uint8Array([0xde, 0xad]),
      });
      await tx.commit().wait({ tier: "global" });
      expect(encrypted.map((entry) => entry.plaintext)).toEqual(packed);
      const root = await db.one(app.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "global",
      });
      expect(root).not.toBeNull();
      const stored = await raw.one(physical.notes.where({ id: note.id }), { tier: "global" });
      expect(hex(stored!.title)).toBe(
        outer + hex(new TextEncoder().encode(root!.epochId)) + inner + "00",
      );
      expect(hex(stored!.payload)).toBe(
        outer + hex(new TextEncoder().encode(root!.epochId)) + inner + "01",
      );
      const decoder = new TextDecoder("utf-8", { fatal: true });
      const columnIds: string[] = [];
      for (const [index, entry] of encrypted.entries()) {
        const transcript = fields(entry.aad);
        expect(transcript).toHaveLength(2);
        expect(hex(transcript[0]!.slice(0, 5))).toBe("4a45324301");
        const context = fields(transcript[0]!.slice(5)).map((field) => decoder.decode(field));
        expect(context).toHaveLength(9);
        expect(context[0]).toBe(
          `["${server.url.replace(/^ws:/, "http:")}/apps/${server.appId}/accounts","dev"]`,
        );
        expect(context.slice(1, 4)).toEqual([
          "jazz.e2ee.cell-record.v1",
          root!.scopeId,
          project.id,
        ]);
        expect(context[4]).toMatch(/^[0-9a-f-]{36}$/);
        expect(context[4]).not.toBe(root!.scopeId);
        expect(context[5]).toBe(note.id);
        expect(context[6]).toMatch(/^[0-9a-f-]{36}$/);
        columnIds.push(context[6]!);
        expect(context.slice(7)).toEqual([root!.epochId, ""]);
        expect(decoder.decode(transcript[1])).toBe(
          index === 0
            ? '{"column_type":{"type":"Text"},"nullable":false}'
            : '{"column_type":{"type":"Bytea"},"nullable":false}',
        );
      }
      expect(new Set(columnIds).size).toBe(2);
      expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual(note);
      expect(decrypted).toHaveLength(2);
      for (const entry of decrypted) expect(entry.aad).toEqual(encrypted[entry.index]!.aad);
    } finally {
      await raw?.shutdown();
      if (owner) await owner.close();
      else await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);
