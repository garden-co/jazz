import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import {
  encodeSchema,
  NativeRuntimeAdapter,
} from "../runtime/native-runtime/native-runtime-adapter.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { frontE2eeCore } from "./fixtures/app.js";
import { transformRow } from "../runtime/row-transformer.js";

it("observes genuine local submissions through its authenticated same-owner subscriber", async () => {
  const app = s.defineApp(
    {
      __e2ee_route_roots: s.table({ title: s.string() }, {}),
      __e2ee_route_refs: s.table({ rootId: s.uuid() }, {}),
    },
    { migrations: "none" },
  );
  const permissions = definePermissions(app, ({ policy }) => {
    policy.__e2ee_route_roots.allowRead.always();
    policy.__e2ee_route_roots.allowInsert.always();
    policy.__e2ee_route_refs.allowRead.always();
    policy.__e2ee_route_refs.allowInsert.always();
  });
  let server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    server = await frontE2eeCore(server, { schema: app, permissions });
    db = await createDb(await localAccountConfig(server.appId, server.url));
    // Establish normal namespace/schema admission before the actual submission.
    // This route test does not alter any cold-cache or first-reopen assertion.
    await db.all(app.__e2ee_route_roots, { tier: "remote" });
    const tx = db.beginSerializableTransaction();
    const root = tx.insert(app.__e2ee_route_roots, { title: "Accepted locally submitted root" });
    const ref = tx.insert(app.__e2ee_route_refs, { rootId: root.id });
    const write = tx.commit();
    await write.wait({ tier: "global" });
    const transactionId = await write.txId;
    expect(app.__e2ee_route_roots._schema.__e2ee_route_roots!.policies).toBeUndefined();
    const runtime = db.currentRuntime();
    expect(runtime).toBeInstanceOf(NativeRuntimeAdapter);
    const owner = runtime as NativeRuntimeAdapter;
    // Preserve the unchanged direct policy-bound denial with no admitted session.
    expect(
      (
        await owner.observeLocalBatch([
          JSON.stringify({ table: "__e2ee_route_roots" }),
          JSON.stringify({ table: "__e2ee_route_refs" }),
        ])
      ).map((x) => x.rows),
    ).toEqual([[], []]);
    // The public typed route uses only this authenticated original owner's peer.
    expect(
      (await db.observeE2eeHistory([app.__e2ee_route_roots, app.__e2ee_route_refs])).map(
        (x) => x.rows,
      ),
    ).toEqual([[root], [ref]]);
    const peer = await owner.acceptPeer();
    const queries = [
      JSON.stringify({ table: "__e2ee_route_roots" }),
      JSON.stringify({ table: "__e2ee_route_refs" }),
    ];
    const schema = encodeSchema(app.__e2ee_route_roots._schema);
    try {
      const observed = await owner.observeLocalBatchForPeer(peer, queries, {}, schema);
      expect(
        observed.map((x, i) =>
          x.rows.map((row) =>
            transformRow(
              row,
              app.__e2ee_route_roots._schema,
              i === 0 ? "__e2ee_route_roots" : "__e2ee_route_refs",
            ),
          ),
        ),
      ).toEqual([[root], [ref]]);
      expect(observed[0]!.through).toBe(observed[1]!.through);
      for (const [index, row] of [root, ref].entries()) {
        expect(observed[index]!.provenance).toEqual(["local-submission"]);
        expect(observed[index]!.evidence).toEqual([
          {
            rowId: row.id,
            state: { transactionId, position: expect.any(String) },
            content: { transactionId, position: expect.any(String) },
          },
        ]);
      }
      await expect(
        owner.observeLocalBatchForPeer(peer, queries, { role: "other" }, schema),
      ).rejects.toThrow(/claims changed/i);
      await expect(owner.acceptPeer({}, undefined, new Uint8Array([9]))).rejects.toMatchObject({
        code: "protocol",
        reason: "identity_mismatch",
      });
      peer.close();
      await expect(owner.observeLocalBatchForPeer(peer, queries, {}, schema)).rejects.toThrow(
        /retired|closed/i,
      );
      await db.shutdown();
      await expect(owner.observeLocalBatchForPeer(peer, queries, {}, schema)).rejects.toThrow(
        /closed/i,
      );
    } finally {
      peer.close();
    }
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);
