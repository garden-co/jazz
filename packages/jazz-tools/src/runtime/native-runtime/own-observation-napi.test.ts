import { expect, it } from "vitest";
import { NapiDb } from "jazz-napi";
import { createJazzSession } from "../../backend/create-jazz-session.js";
import { schema as s } from "../../schema-namespace.js";
import { definePermissions } from "../../permissions/index.js";
import { startTestJazz } from "../../testing/index.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import {
  trustedReservedSessionToken,
  TRUSTED_RESERVED_SESSION_TOKEN_FIELD,
} from "../client-session.js";
import { getDbInternalSession } from "../db-internal-session.js";
import { translateQuery } from "../query-adapter.js";
import { transformRow } from "../row-transformer.js";

it("observes a genuine native account's accepted rows and refuses its retired owner", async () => {
  const app = s.defineApp(
    { observation_rows: s.table({ ownerAccountId: s.uuid(), title: s.string() }, {}) },
    { migrations: "none" },
  );
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.observation_rows.allowRead.where({ ownerAccountId: session.user.account });
    policy.observation_rows.allowInsert.where({ ownerAccountId: session.user.account });
  });
  const server = await startTestJazz({
    app,
    permissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  let stored: string | null = null;
  let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  try {
    expect(typeof NapiDb.prototype.acceptOwnObservationSubscriber).toBe("function");
    owner = await createJazzSession({
      appId: server.appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "memory" },
      initial: "local-first",
      store: {
        async read() {
          return stored;
        },
        async update(transform) {
          stored = transform(stored);
        },
      },
    });
    const db = owner.getSnapshot().client!.db;
    const accountId = owner.getSnapshot().account!.id;
    await db.all(app.observation_rows, { tier: "remote" });
    const write = db.insert(app.observation_rows, {
      ownerAccountId: accountId,
      title: "Native owner row",
    });
    await write.wait({ tier: "global" });
    const query = app.observation_rows.where({ id: write.value.id });
    const actual = getDbInternalSession(db);
    if (!actual) throw new Error("Missing real native account session");
    const session = JSON.stringify({
      account_id: actual.account_id,
      issuer: actual.issuer,
      user_id: actual.user_id,
      authMode: actual.authMode,
      claims: actual.claims,
      [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]: trustedReservedSessionToken(actual),
    });
    const runtime = db.currentRuntime() as NativeRuntimeAdapter;
    const encoded = translateQuery(query._build(), query._schema);
    const observed = await runtime.observeLocalBatch([encoded, encoded], session);
    expect(
      observed.map((x) => x.rows.map((row) => transformRow(row, query._schema, query._table))),
    ).toEqual([[write.value], [write.value]]);
    expect(observed[0]!.through).toBe(observed[1]!.through);
    expect(observed[0]!.provenance).toEqual(["local-submission"]);
    expect(observed[0]!.evidence[0]!.rowId).toBe(write.value.id);
    await owner.close();
    owner = undefined;
    await expect(runtime.observeLocalBatch([encoded], session)).rejects.toThrow(/closed/);
  } finally {
    await owner?.close();
    await server.stop();
  }
}, 30_000);
