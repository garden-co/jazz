import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createNativeCrypto } from "./native.js";

it.each(["retired-empty-root", "lost-required-space"] as const)(
  "rechecks native space recovery creation coverage (%s)",
  async (change) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
    });
    const policies = definePermissions(app, ({ policy, session }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.projects.allowRead.where(authenticated);
      policy.projects.allowInsert.where(authenticated);
      policy.__e2ee_spaces.allowRead.where(authenticated);
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.where(authenticated);
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.where(authenticated);
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.where(authenticated);
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_space_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
    });
    const permissions = { ...deviceRequestPermissions, ...policies };
    const store = () => {
      let value: string | null = null;
      return {
        async read() {
          return value;
        },
        async update(transform: (current: string | null) => string) {
          value = transform(value);
        },
      };
    };
    const accountStore = store();
    const sessions: Awaited<ReturnType<typeof createJazzSession>>[] = [];
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      const native = await createNativeCrypto();
      const open = async (crypto = native) => {
        const session = await createJazzSession({
          appId: server.appId,
          serverUrl: server.url,
          app,
          permissions,
          driver: { type: "memory" },
          initial: "local-first",
          store: accountStore,
          e2ee: { app, store: store(), crypto },
        });
        sessions.push(session);
        return session;
      };
      let beforeImport: ((rootId: string) => Promise<void>) | undefined;
      let changedRoot: string | undefined;
      let changeAccepted = false;
      const creator = await open({
        ...native,
        keyEnvelope: {
          ...native.keyEnvelope,
          async open(pair, context, envelope) {
            const opened = await native.keyEnvelope.open(pair, context, envelope);
            const text = new TextDecoder().decode(context);
            if (beforeImport && text.startsWith("jazz.e2ee.recovery-material-check.v1\0")) {
              const action = beforeImport;
              beforeImport = undefined;
              const rootId = text.slice(text.lastIndexOf("\0") + 1);
              try {
                await action(rootId);
                changedRoot = rootId;
                changeAccepted = true;
              } catch (error) {
                opened.fill(0);
                throw error;
              }
            }
            return opened;
          },
        },
      });
      const db = creator.getSnapshot().client!.db;
      const accountId = creator.getSnapshot().account!.id;
      await db.e2ee.devices.list();
      const other = await open();
      const peer = other.getSnapshot().client!.db;
      const pending = (await peer.e2ee.devices.list()).find((device) => device.state === "pending");
      expect(pending).toBeDefined();
      await db.e2ee.devices.approve(pending!.id).wait();
      expect(await peer.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: pending!.id, state: "active" }),
      );
      let identifier: string | undefined;
      if (change === "lost-required-space") {
        const project = await db
          .insert(app.projects, { title: "Required during creation" })
          .wait({ tier: "global" });
        identifier = project.id;
        await db.e2ee.spaces.grant(app.projects, project.id, accountId).wait();
        expect(await db.e2ee.explain({ scope: app.projects, identifier })).toEqual({
          state: "ready",
        });
      } else {
        expect(await db.all(app.__e2ee_spaces, { tier: "global" })).toEqual([]);
      }
      beforeImport = async (rootId) => {
        expect(
          await peer.one(app.__e2ee_recovery_roots.where({ id: rootId }), { tier: "global" }),
        ).not.toBeNull();
        // Observe the accepted change on the creating client before import resumes.
        if (change === "retired-empty-root") {
          await peer.e2ee.recovery.revoke(rootId).wait();
          expect((await db.e2ee.recovery.status()).account.recoveryRootIds).not.toContain(rootId);
        } else {
          await peer.e2ee.spaces.revoke(app.projects, identifier!, accountId).wait();
          expect(
            await db.e2ee.explain({ scope: app.projects, identifier: identifier! }),
          ).toMatchObject({
            state: "refused",
            reason: "space-sealed",
          });
        }
      };
      const outcome = await db.e2ee.recovery
        .create()
        .wait()
        .then(
          (value) => ({ ok: true as const, value }),
          (error: unknown) => ({ ok: false as const, error }),
        );
      expect(changeAccepted).toBe(true);
      expect(changedRoot).toBeDefined();
      expect(outcome.ok).toBe(false);
      if (outcome.ok) throw new Error("Creation returned material after its coverage changed");
      if (change === "retired-empty-root")
        expect(outcome.error).toMatchObject({ code: "recovery-root-mismatch" });
      else expect(outcome.error).toMatchObject({ message: "Recovery space is unavailable" });
      expect(
        await db.all(app.__e2ee_recovery_protectors.where({ rootId: changedRoot! }), {
          tier: "global",
        }),
      ).toEqual([]);
      const good = await db.e2ee.recovery.create().wait();
      expect(await db.e2ee.recovery.status(good.material)).toMatchObject({
        account: { validation: "validated" },
        spaces: { validation: "checked", paths: [] },
      });
    } finally {
      await Promise.all(sessions.map((session) => session.close()));
      await server.stop();
    }
  },
  120_000,
);
