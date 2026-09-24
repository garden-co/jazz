import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { exclusiveE2eeTransaction, type Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { prepareStreamingSpace, prepareInitialSpaceRows } from "./lifecycle.js";
import { createBrowserKeyEnvelope, createBrowserDeviceSigner } from "./browser.js";
import { E2eeDataError } from "./data-error.js";

it.each([false, true])(
  "hands the exact unpublished seed to one transaction (exclude creator: %s)",
  async (excludeCreator) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
    });
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Db | undefined;
    let recipient: Db | undefined;
    let saved: string | null = null;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...permissions },
      });
      const keys = await createBrowserKeyEnvelope();
      const signer = await createBrowserDeviceSigner();
      let failWrap = false;
      const adapterFailure = new Error("private-key-sentinel-must-not-escape");
      db = await createDb({
        ...(await localAccountConfig(server.appId, server.url)),
        e2ee: {
          app,
          crypto: {
            deviceSigner: signer,
            keyEnvelope: {
              ...keys,
              async wrap(...args) {
                if (failWrap) throw adapterFailure;
                return keys.wrap(...args);
              },
            },
          },
          store: {
            async read() {
              return saved;
            },
            async update(transform) {
              saved = transform(saved);
            },
          },
        },
      });
      await db.e2ee.devices.list();
      let recipientId: string | undefined;
      if (excludeCreator) {
        const config = await localAccountConfig(server.appId, server.url);
        recipientId = config.account.id;
        let retained: string | null = null;
        recipient = await createDb({
          ...config,
          e2ee: {
            app,
            store: {
              async read() {
                return retained;
              },
              async update(transform) {
                retained = transform(retained);
              },
            },
          },
        });
        await recipient.e2ee.devices.list();
      }
      const project = await db.insert(app.projects, { title: "Seed" }).wait({ tier: "global" });
      failWrap = true;
      await expect(
        prepareStreamingSpace(db, app.projects, project.id, {}, async () => {}),
      ).rejects.toEqual(new E2eeDataError("key-unavailable"));
      failWrap = false;
      const stageFailure = new Error("caller source unavailable");
      await expect(
        prepareStreamingSpace(db, app.projects, project.id, {}, async () => {
          throw stageFailure;
        }),
      ).rejects.toBe(stageFailure);
      const prepared = await prepareStreamingSpace(
        db,
        app.projects,
        project.id,
        recipientId ? { initialRecipients: [recipientId] } : {},
        async (key, root) => ({ key: key.slice(), epoch: root.epochId }),
      );
      try {
        expect(await db.all(app.__e2ee_spaces, { tier: "global" })).toEqual([]);
        const owner = db;
        const write = await exclusiveE2eeTransaction(db, async (tx) => {
          await prepared.plan.validate(tx);
          await prepareInitialSpaceRows(
            owner,
            tx,
            app.projects,
            project.id,
            async (key, root) => {
              expect(key).toEqual(prepared.value.key);
              expect(root.epochId).toBe(prepared.value.epoch);
            },
            undefined,
            prepared.plan.initialSeed,
          );
        });
        await write.wait({ tier: "global" });
        expect(
          (await db.one(app.__e2ee_spaces.where({ identifier: project.id }), { tier: "global" }))
            ?.epochId,
        ).toBe(prepared.value.epoch);
        await expect(
          exclusiveE2eeTransaction(db, (tx) =>
            prepareInitialSpaceRows(
              owner,
              tx,
              app.projects,
              project.id,
              async () => {},
              undefined,
              prepared.plan.initialSeed,
            ),
          ),
        ).rejects.toThrow();
        if (recipientId) {
          const root = await db.one(app.__e2ee_spaces.where({ identifier: project.id }), {
            tier: "global",
          });
          expect(
            (
              await db.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), { tier: "global" })
            ).map((grant) => grant.recipientId),
          ).toEqual([recipientId]);
          let used = false;
          await expect(
            prepareStreamingSpace(db, app.projects, project.id, {}, async () => {
              used = true;
            }),
          ).rejects.toThrow();
          expect(used).toBe(false);
        } else {
          const stale = await prepareStreamingSpace(
            db,
            app.projects,
            project.id,
            {},
            async (_key, root) => root.epochId,
          );
          try {
            const verify = signer.verify;
            try {
              signer.verify = async () => {
                throw adapterFailure;
              };
              await expect(
                exclusiveE2eeTransaction(db, (tx) => stale.plan.validate(tx)),
              ).rejects.toEqual(new E2eeDataError("key-unavailable"));
            } finally {
              signer.verify = verify;
            }
            const root = await db.one(app.__e2ee_spaces.where({ identifier: project.id }), {
              tier: "global",
            });
            await db.e2ee.spaces.revoke(app.projects, project.id, root!.accountId).wait();
            await expect(
              exclusiveE2eeTransaction(db, (tx) => stale.plan.validate(tx)),
            ).rejects.toThrow();
          } finally {
            stale.plan.dispose();
          }
        }
      } finally {
        prepared.value.key.fill(0);
        prepared.plan.dispose();
      }
    } finally {
      await recipient?.shutdown();
      await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);
