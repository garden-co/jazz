import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserCrypto } from "./browser.js";
import { createNativeCrypto } from "./native.js";
import { spaceContext, spaceGrantBytes } from "./space-format.js";
import { spaceSuccessorBytes, spaceSuccessorContext } from "./space-successor.js";
import { encodePublicApprovalRevision } from "./account-successor.js";
import { encodeGroupMembership } from "./group-successor.js";
import { beginInitialSpaceTransaction, prepareInitialSpaceForTransaction } from "./lifecycle.js";

it.each(
  ["wasm", "native"].flatMap((runtime) =>
    [false, true].map((successor) => ({ runtime, successor })),
  ),
)(
  "ignores ineligible initial lifecycle rows ($runtime, successor=$successor)",
  async ({ runtime, successor }) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
    });
    const policies = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      // Deliberately admit adversarial rows: E2EE replay must still authenticate them.
      policy.__e2ee_space_grants.allowInsert.always();
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    let nativeSession: Awaited<ReturnType<typeof createJazzSession>> | undefined;
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
    const stores = [store(), store(), store()];
    const crypto = await (runtime === "native" ? createNativeCrypto() : createBrowserCrypto());
    const signingKeys: Uint8Array[] = [];
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...policies },
      });
      let creator: Awaited<ReturnType<typeof createDb>>;
      let creatorId: string;
      if (runtime === "native") {
        nativeSession = await createJazzSession({
          appId: server.appId,
          serverUrl: server.url,
          app,
          permissions: { ...deviceRequestPermissions, ...policies },
          driver: { type: "memory" },
          initial: "local-first",
          e2ee: { app, crypto, store: stores[0]! },
        });
        creator = nativeSession.getSnapshot().client!.db;
        creatorId = nativeSession.getSnapshot().account!.id;
      } else {
        const account = await localAccountConfig(server.appId, server.url);
        creator = await createDb({ ...account, e2ee: { app, crypto, store: stores[0]! } });
        creatorId = account.account.id;
      }
      clients.push(creator);
      const recipientIds = [creatorId];
      for (let index = 1; index < 3; index++) {
        const account = await localAccountConfig(server.appId, server.url);
        recipientIds.push(account.account.id);
        clients.push(await createDb({ ...account, e2ee: { app, store: stores[index]! } }));
      }
      for (const client of clients) await client.e2ee.devices.list();
      const devices = await Promise.all(
        stores.map(async (entry) => JSON.parse((await entry.read())!).devices[0]),
      );
      for (const device of devices) signingKeys.push(Uint8Array.from(device.signingPrivateKey));
      const identities = await Promise.all(
        recipientIds.map((id) =>
          creator.one(app.__e2ee_account_roots.where({ accountId: id }), { tier: "global" }),
        ),
      );
      const tx = beginInitialSpaceTransaction(creator, recipientIds);
      for (const identity of identities) expect(identity).not.toBeNull();
      const project = tx.insert(app.projects, { title: "Adversarial initial grants" });
      const injectedIds: string[] = [];
      let initialEpoch = "";
      let successorId = "";
      await prepareInitialSpaceForTransaction(
        creator,
        tx,
        app.projects,
        project.id,
        async (key, root, prepared) => {
          initialEpoch = root.epochId;
          if (successor) {
            const nextKey = globalThis.crypto.getRandomValues(new Uint8Array(32));
            try {
              const coordinates = {
                id: globalThis.crypto.randomUUID(),
                spaceId: root.id,
                predecessor: root.epochId,
                epochId: globalThis.crypto.randomUUID(),
                authorAccountId: creatorId,
                authorDeviceId: root.deviceId,
                authorEpochId: root.accountEpochId,
              };
              const application = devices[0].scope;
              const historyContext = spaceSuccessorContext(
                application,
                root,
                coordinates,
                "history",
              );
              const verificationContext = spaceContext(
                application,
                { ...root, epochId: coordinates.epochId },
                "verification",
              );
              const history = await crypto.keyEnvelope.wrap(nextKey, historyContext, key);
              const verification = await crypto.keyEnvelope.wrap(
                nextKey,
                verificationContext,
                new Uint8Array(32),
              );
              const previous = await crypto.keyEnvelope.unwrap(nextKey, historyContext, history);
              try {
                expect(previous).toEqual(key);
              } finally {
                previous.fill(0);
              }
              expect(
                await crypto.keyEnvelope.unwrap(nextKey, verificationContext, verification),
              ).toEqual(new Uint8Array(32));
              const proposal = {
                ...coordinates,
                revision: encodePublicApprovalRevision(
                  (
                    await prepared.all(app.__e2ee_space_grants.where({ spaceId: root.id }), {
                      tier: "local",
                    })
                  ).map((grant) => "space-grant:" + grant.id),
                ),
                membership: encodeGroupMembership(
                  new Map(
                    recipientIds.slice(0, 2).map((id, index) => [id, identities[index]!.epochId]),
                  ),
                ),
                history,
                verification,
                authorEnvelope: await crypto.keyEnvelope.seal(
                  Uint8Array.from(devices[0].publicKey),
                  spaceSuccessorContext(
                    application,
                    root,
                    coordinates,
                    "author-envelope",
                    root.deviceId,
                  ),
                  nextKey,
                ),
              };
              const bytes = spaceSuccessorBytes(application, root, proposal);
              const signature = await crypto.deviceSigner.sign(signingKeys[0]!, bytes);
              expect(
                await crypto.deviceSigner.verify(
                  Uint8Array.from(devices[0].signingPublicKey),
                  bytes,
                  signature,
                ),
              ).toBe(true);
              const { id, ...values } = proposal;
              prepared.insert(app.__e2ee_space_successors, { ...values, signature }, { id });
              successorId = id;
            } finally {
              nextKey.fill(0);
            }
          }
          for (const variant of [
            "other-author",
            "remove",
            "space-epoch",
            "author-epoch",
            "recipient-epoch",
            "signature",
          ]) {
            const author = variant === "other-author" ? 1 : 0;
            const recipient = variant === "remove" ? 0 : 2;
            const grant = {
              id: globalThis.crypto.randomUUID(),
              spaceId: root.id,
              epochId: variant === "space-epoch" ? globalThis.crypto.randomUUID() : root.epochId,
              authorAccountId: recipientIds[author]!,
              authorDeviceId: devices[author].id as string,
              authorEpochId:
                variant === "author-epoch"
                  ? globalThis.crypto.randomUUID()
                  : identities[author]!.epochId,
              operation: variant === "remove" ? "remove" : "add",
              recipientKind: "account",
              recipientId: recipientIds[recipient]!,
              recipientEpochId:
                variant === "recipient-epoch"
                  ? globalThis.crypto.randomUUID()
                  : identities[recipient]!.epochId,
            };
            const bytes = spaceGrantBytes(
              devices[0].scope,
              { ...root, epochId: grant.epochId },
              grant,
            );
            const signature = await crypto.deviceSigner.sign(signingKeys[author]!, bytes);
            expect(
              await crypto.deviceSigner.verify(
                Uint8Array.from(devices[author].signingPublicKey),
                bytes,
                signature,
              ),
            ).toBe(true);
            if (variant === "signature") signature[0] = signature[0]! ^ 1;
            const { id, ...values } = grant;
            prepared.insert(app.__e2ee_space_grants, { ...values, signature }, { id });
            injectedIds.push(id);
          }
        },
        recipientIds.slice(0, 2),
      );
      expect(await creator.all(app.__e2ee_space_deliveries, { tier: "global" })).toEqual([]);
      await tx.commit().wait({ tier: "global" });
      const grants = await creator.all(app.__e2ee_space_grants, { tier: "global" });
      expect(grants).toHaveLength(8);
      expect(injectedIds.every((id) => grants.some((row) => row.id === id))).toBe(true);
      const target = { scope: app.projects, identifier: project.id };
      expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
      expect(await clients[1]!.e2ee.explain(target)).toEqual({ state: "ready" });
      expect(await clients[2]!.e2ee.explain(target)).toEqual({
        state: "refused",
        reason: "not-a-space-recipient",
      });
      const deliveries = await creator.all(app.__e2ee_space_deliveries, { tier: "global" });
      expect([...new Set(deliveries.map((row) => row.recipientAccountId))].sort()).toEqual(
        recipientIds.slice(0, 2).sort(),
      );
      expect(deliveries.every((row) => row.epochId === initialEpoch)).toBe(true);
      const successors = await creator.all(app.__e2ee_space_successors, { tier: "global" });
      if (successor) {
        expect(successors).toHaveLength(1);
        expect(successors[0]).toMatchObject({ id: successorId, predecessor: initialEpoch });
      } else expect(successors).toEqual([]);
    } finally {
      for (const key of signingKeys) key.fill(0);
      await Promise.all(clients.map((client) => client.shutdown()));
      await nativeSession?.close();
      await server.stop();
    }
  },
  60_000,
);
