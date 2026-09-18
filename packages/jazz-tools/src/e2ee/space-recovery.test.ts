import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import fixture from "./fixtures/local-space-recovery-v1.json";

for (const scenario of [
  "ordinary",
  "read-only-recovered-device",
  "malformed-recovery-candidates",
  "unrelated-invalid-root",
] as const) {
  it(
    `recovers an existing space after losing the only enrolled device (${scenario})`,
    async () => {
      const app = s.defineApp({
        ...deviceRequestSchema,
        ...spaceSchema,
        projects: s.table({ title: s.string() }, {}),
      });
      const policies = definePermissions(app, ({ policy, session, allOf }) => {
        const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
        policy.projects.allowRead.where(authenticated);
        policy.projects.allowInsert.where(authenticated);
        policy.__e2ee_spaces.allowRead.where(authenticated);
        policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
        policy.__e2ee_space_grants.allowRead.where(authenticated);
        policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
        if (scenario === "unrelated-invalid-root")
          policy.__e2ee_space_grants.allowDelete.where({ authorAccountId: session.user.account });
        policy.__e2ee_space_deliveries.allowRead.where(authenticated);
        if (scenario === "read-only-recovered-device") {
          policy.__e2ee_space_deliveries.allowInsert.where((row) =>
            allOf([
              { senderAccountId: session.user.account },
              policy.__e2ee_spaces.exists.where({ id: row.spaceId, deviceId: row.senderDeviceId }),
            ]),
          );
        } else
          policy.__e2ee_space_deliveries.allowInsert.where({
            senderAccountId: session.user.account,
          });
        policy.__e2ee_space_successors.allowRead.where(authenticated);
        policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
        policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
        policy.__e2ee_space_recovery_deliveries.allowInsert.where({
          senderAccountId: session.user.account,
        });
      });
      const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
      const clients: Awaited<ReturnType<typeof createDb>>[] = [];
      const store = () => {
        let saved: string | null = JSON.stringify(fixture);
        return {
          async read() {
            return saved;
          },
          async update(transform: (current: string | null) => string) {
            saved = transform(saved);
          },
        };
      };
      try {
        await deploy({
          serverUrl: server.url,
          appId: server.appId,
          adminSecret: server.adminSecret,
          schema: app,
          permissions: { ...deviceRequestPermissions, ...policies },
        });
        const account = await localAccountConfig(server.appId, server.url);
        const first = await createDb({ ...account, e2ee: { app, store: store() } });
        clients.push(first);
        await first.e2ee.devices.list();
        const project = await first
          .insert(app.projects, { title: "Recoverable space" })
          .wait({ tier: "global" });
        await first.e2ee.spaces.grant(app.projects, project.id, account.account.id).wait();
        const target = { scope: app.projects, identifier: project.id };
        expect(await first.e2ee.explain(target)).toEqual({ state: "ready" });
        const { material } = await first.e2ee.recovery.create().wait();
        if (scenario === "unrelated-invalid-root") {
          const [root] = await first.all(app.__e2ee_spaces, { tier: "edge" });
          expect(root).toBeDefined();
          const other = await first
            .insert(app.projects, { title: "Unrelated scope" })
            .wait({ tier: "global" });
          const { id: _id, ...values } = root!;
          // Ordinary row policy does not authenticate the E2EE transcript. Copying
          // a root to another space invalidates its signature, even for its author.
          await first
            .insert(app.__e2ee_spaces, {
              ...values,
              identifier: other.id,
              initialGrantId: crypto.randomUUID(),
            })
            .wait({ tier: "global" });
          const status = await first.e2ee.recovery.status(material);
          expect(status.spaces).toEqual({
            validation: "checked",
            paths: [
              {
                scopeId: root!.scopeId,
                identifier: project.id,
                spaceId: root!.id,
                epochId: root!.epochId,
                validation: "validated",
              },
            ],
          });
          // Creation and later use must discover the same authenticated spaces.
          await first.e2ee.recovery.create().wait();
        }
        if (scenario === "malformed-recovery-candidates") {
          const writer = await createDb({ ...account });
          clients.push(writer);
          const [root] = await writer.all(app.__e2ee_spaces, { tier: "edge" });
          const [recovery] = await writer.all(app.__e2ee_recovery_roots, { tier: "edge" });
          expect(root).toBeDefined();
          expect(recovery).toBeDefined();
          // Both rows coexist with the genuine envelope under ordinary policy.
          // UUID v1 is a valid Jazz ID but not a valid E2EE v1 transcript ID;
          // the second candidate has valid coordinates but an invalid signature.
          for (const id of ["00000000-0000-1000-8000-000000000001", crypto.randomUUID()]) {
            await writer
              .insert(
                app.__e2ee_space_recovery_deliveries,
                {
                  spaceId: root!.id,
                  epochId: root!.epochId,
                  senderAccountId: account.account.id,
                  senderDeviceId: root!.deviceId,
                  senderEpochId: root!.accountEpochId,
                  recipientAccountId: account.account.id,
                  recipientEpochId: root!.accountEpochId,
                  recoveryRootId: recovery!.id,
                  envelope: Uint8Array.of(1),
                  signature: new Uint8Array(64),
                },
                { id },
              )
              .wait({ tier: "global" });
          }
          expect(await first.e2ee.explain(target)).toEqual({ state: "ready" });
          expect(
            await first.all(app.__e2ee_space_recovery_deliveries, { tier: "edge" }),
          ).toHaveLength(3);
        }
        await first.shutdown();

        // Only exported recovery material survives: no old device store or live key holder.
        const replacementStore = store();
        const replacement = await createDb({ ...account, e2ee: { app, store: replacementStore } });
        clients.push(replacement);
        const pending = (await replacement.e2ee.devices.list()).find(
          (device) => device.state === "pending",
        );
        expect(pending).toBeDefined();
        expect(await replacement.e2ee.explain(target)).toMatchObject({ state: "refused" });
        await replacement.e2ee.recovery.use(material).wait();
        expect(await replacement.e2ee.devices.list()).toContainEqual(
          expect.objectContaining({ id: pending!.id, state: "active" }),
        );
        expect(await replacement.e2ee.explain(target)).toEqual({ state: "ready" });
        // Existing private-store entries survive; recovered candidates use the pinned format.
        const retained = JSON.parse((await replacementStore.read())!);
        expect(retained.recoveredSpaceKeysV1).toHaveLength(2);
        expect(retained.recoveredSpaceKeysV1).toContainEqual(fixture.recoveredSpaceKeysV1[0]);
        if (scenario === "read-only-recovered-device") {
          expect(
            await replacement.all(
              app.__e2ee_space_deliveries.where({ recipientDeviceId: pending!.id }),
              { tier: "edge" },
            ),
          ).toEqual([]);
        }
        await replacement.shutdown();
        const reopened = await createDb({ ...account, e2ee: { app, store: replacementStore } });
        clients.push(reopened);
        expect(await reopened.e2ee.explain(target)).toEqual({ state: "ready" });
        if (scenario === "unrelated-invalid-root") {
          const [root] = await reopened.all(app.__e2ee_spaces.where({ identifier: project.id }), {
            tier: "edge",
          });
          expect(root).toBeDefined();
          await reopened
            .delete(app.__e2ee_space_grants, root!.initialGrantId)
            .wait({ tier: "global" });
          // This root's signature is genuine. Incomplete membership is not evidence
          // that the caller has no recovery obligations, so discovery must fail closed.
          await expect(reopened.e2ee.recovery.status(material)).rejects.toThrow(
            "Invalid or unsupported E2EE space membership",
          );
        }
      } finally {
        await Promise.all(clients.map((client) => client.shutdown()));
        await server.stop();
      }
    },
    scenario === "unrelated-invalid-root" ? 180_000 : 60_000,
  );
}
