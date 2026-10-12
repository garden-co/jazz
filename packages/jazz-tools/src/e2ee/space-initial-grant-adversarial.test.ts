import { afterAll, beforeAll, describe, expect, it } from "vitest";
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

async function createFixture(runtime: "wasm" | "native") {
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
  const signingKeys: Uint8Array[] = [];
  const close = async () => {
    for (const key of signingKeys) key.fill(0);
    await Promise.all(clients.map((client) => client.shutdown()));
    await nativeSession?.close();
    await server.stop();
  };
  try {
    const crypto = await (runtime === "native" ? createNativeCrypto() : createBrowserCrypto());
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
        creator.one(app.__e2ee_account_roots.where({ accountId: id }), { tier: "remote" }),
      ),
    );
    for (const identity of identities) expect(identity).not.toBeNull();
    return {
      app,
      creator,
      creatorId,
      clients,
      recipientIds,
      devices,
      identities,
      crypto,
      signingKeys,
      close,
    };
  } catch (error) {
    await close();
    throw error;
  }
}

// Reuse enrolled accounts within each runtime. Every case prepares a new space,
// and checks that its adversarial history leaves earlier spaces untouched.
describe.each(["wasm", "native"] as const)("initial lifecycle rows (%s)", (runtime) => {
  let fixture: Awaited<ReturnType<typeof createFixture>>;
  beforeAll(async () => {
    fixture = await createFixture(runtime);
  }, 60_000);
  afterAll(async () => {
    await fixture?.close();
  });
  it.each([false, true])(
    "ignores ineligible initial lifecycle rows (successor=%s)",
    async (successor) => {
      const {
        app,
        creator,
        creatorId,
        clients,
        recipientIds,
        devices,
        identities,
        crypto,
        signingKeys,
      } = fixture;
      const before = {
        grants: await creator.all(app.__e2ee_space_grants, { tier: "remote" }),
        deliveries: await creator.all(app.__e2ee_space_deliveries, { tier: "remote" }),
        successors: await creator.all(app.__e2ee_space_successors, { tier: "remote" }),
      };
      const tx = beginInitialSpaceTransaction(creator, recipientIds);
      const project = tx.insert(app.projects, { title: "Adversarial initial grants" });
      const injectedIds: string[] = [];
      let spaceId = "";
      let initialEpoch = "";
      let successorId = "";
      await prepareInitialSpaceForTransaction(
        creator,
        tx,
        app.projects,
        project.id,
        async (key, root, prepared) => {
          spaceId = root.id;
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
                      tier: "local-first",
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
      expect(await creator.all(app.__e2ee_space_deliveries, { tier: "remote" })).toEqual(
        before.deliveries,
      );
      await tx.commit().wait({ tier: "global" });
      const allGrants = await creator.all(app.__e2ee_space_grants, { tier: "remote" });
      const grants = allGrants.filter((row) => row.spaceId === spaceId);
      expect(grants).toHaveLength(8);
      expect(injectedIds.every((id) => grants.some((row) => row.id === id))).toBe(true);
      const target = { scope: app.projects, identifier: project.id };
      expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
      expect(await clients[1]!.e2ee.explain(target)).toEqual({ state: "ready" });
      expect(await clients[2]!.e2ee.explain(target)).toEqual({
        state: "refused",
        reason: "not-a-space-recipient",
      });
      const allDeliveries = await creator.all(app.__e2ee_space_deliveries, { tier: "remote" });
      const deliveries = allDeliveries.filter((row) => row.spaceId === spaceId);
      expect([...new Set(deliveries.map((row) => row.recipientAccountId))].sort()).toEqual(
        recipientIds.slice(0, 2).sort(),
      );
      expect(deliveries.every((row) => row.epochId === initialEpoch)).toBe(true);
      const allSuccessors = await creator.all(app.__e2ee_space_successors, { tier: "remote" });
      const successors = allSuccessors.filter((row) => row.spaceId === spaceId);
      if (successor) {
        expect(successors).toHaveLength(1);
        expect(successors[0]).toMatchObject({ id: successorId, predecessor: initialEpoch });
      } else expect(successors).toEqual([]);
      for (const [rows, previous] of [
        [allGrants, before.grants],
        [allDeliveries, before.deliveries],
        [allSuccessors, before.successors],
      ] as const) {
        const unrelated = rows.filter((row) => row.spaceId !== spaceId);
        expect(unrelated).toHaveLength(previous.length);
        expect(unrelated).toEqual(expect.arrayContaining<unknown>(previous));
      }
    },
    60_000,
  );
});
