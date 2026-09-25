import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer, startTestJwtIssuer } from "../testing/index.js";
import { resolveSchemaSource } from "../schema-source.js";
import { JazzClient as RuntimeClient } from "../runtime/client.js";
import type { Db } from "../runtime/db.js";
import { createJazzSession } from "./index.js";
import type { JazzClient } from "./create-jazz-session.js";
import type { JazzSession } from "../session/state.js";

it("reopens an admitted external account and settles its exact locally published initialization", async () => {
  const root = await mkdtemp(join(tmpdir(), "jazz-account-owner-"));
  const issuer = await startTestJwtIssuer();
  const appId = randomUUID();
  const auth = { jwksUrl: issuer.jwksUrl, jwtIssuer: issuer.issuer, jwtAudience: issuer.audience };
  const server = await startLocalJazzServer({ appId, ...auth });
  const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
  const permissions = s.definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
  });
  const config = {
    appId,
    serverUrl: server.url,
    app,
    permissions,
    ...auth,
    driver: { type: "persistent" as const, dataPath: join(root, "client") },
  };
  let owner: JazzSession<JazzClient> | undefined;
  const runtimeClient = (db: Db): RuntimeClient => {
    const getClient: unknown = Reflect.get(db, "getCurrentClient");
    if (typeof getClient !== "function") throw new Error("Missing Db runtime-owner seam");
    const candidate: unknown = Reflect.apply(getClient, db, []);
    if (!(candidate instanceof RuntimeClient))
      throw new Error("Expected the host's native runtime client");
    return candidate;
  };
  try {
    await deploy({
      serverUrl: server.url,
      appId,
      adminSecret: server.adminSecret,
      schema: resolveSchemaSource(app),
      permissions,
    });
    const token = await issuer.jwtForUser("external-owner-restart");
    owner = await createJazzSession(config);
    await owner.loginOrRegisterJWT(token);
    const first = owner.getSnapshot().client!;
    const accountId = owner.getSnapshot().account!.id;
    await first.db.insert(app.notes, { text: "authenticated warm-up" }).wait({ tier: "global" });
    await first.db.disconnect();
    const client = runtimeClient(first.db);
    const open = client.beginTransaction("exclusive");
    const rowId = randomUUID();
    await client.prepareTransaction(open, async (io) => {
      await io.recordInitializationInsertAbsence("notes", rowId);
      io.insertInternal(
        "notes",
        { text: { type: "Text", value: "exact pending initialization" } },
        { id: rowId },
      );
    });
    const seal = await client.sealInitializationTransaction(open);
    const published = await client.publishInitializationTransaction(seal);
    await client.waitForTransaction(published, "local");
    expect(await client.initializationTransactionStatus([seal.reservedTxId])).toEqual([
      {
        kind: "complete",
        reservedTxId: seal.reservedTxId,
        fate: { kind: "pending" },
        durability: "local",
      },
    ]);
    await owner.close();
    owner = await createJazzSession(config);
    await owner.loginJWT(token);
    expect(owner.getSnapshot().account!.id).toBe(accountId);
    const reopened = owner.getSnapshot().client!;
    expect(await reopened.db.one(app.notes.where({ id: rowId }))).toMatchObject({
      id: rowId,
      text: "exact pending initialization",
    });
    await vi.waitFor(
      async () => {
        expect(
          await runtimeClient(reopened.db).initializationTransactionStatus([seal.reservedTxId]),
        ).toEqual([
          {
            kind: "complete",
            reservedTxId: seal.reservedTxId,
            fate: { kind: "accepted" },
            durability: "global",
          },
        ]);
      },
      { timeout: 10_000 },
    );
  } finally {
    await owner?.close();
    await server.stop();
    await issuer.stop();
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);
