import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserCrypto } from "./browser.js";
import { spaceContext, spaceDeliveryContext } from "./space-format.js";
import { spaceSuccessorBytes, spaceSuccessorContext } from "./space-successor.js";
import { encodePublicApprovalRevision } from "./account-successor.js";
import { encodeGroupMembership } from "./group-successor.js";

it("rejects a removed author, replays successive rotations and delivers history to a new device", async () => {
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
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
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
  const secrets: Uint8Array[] = [];
  let failNextWrap = false;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: {
        ...deviceRequestPermissions,
        projects: policies.projects!,
        __e2ee_spaces: policies.__e2ee_spaces!,
        __e2ee_space_grants: policies.__e2ee_space_grants!,
        __e2ee_space_deliveries: policies.__e2ee_space_deliveries!,
        __e2ee_space_successors: policies.__e2ee_space_successors!,
      },
    });
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const crypto = await createBrowserCrypto();
    const keys = crypto.keyEnvelope;
    const signer = crypto.deviceSigner;
    const aliceStore = store();
    const bobStore = store();
    const creator = await createDb({
      ...alice,
      e2ee: {
        app,
        store: aliceStore,
        crypto: {
          ...crypto,
          keyEnvelope: {
            ...keys,
            async wrap(key, context, value) {
              if (failNextWrap) {
                failNextWrap = false;
                throw new Error("fixture: pause successor creation");
              }
              return keys.wrap(key, context, value);
            },
          },
        },
      },
    });
    const removed = await createDb({ ...bob, e2ee: { app, store: bobStore } });
    clients.push(creator, removed);
    const [first] = await creator.e2ee.devices.list();
    const [bobDevice] = await removed.e2ee.devices.list();
    const remaining = await createDb({ ...alice, e2ee: { app, store: store() } });
    clients.push(remaining);
    const request = (await remaining.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await creator.e2ee.devices.approve(request.id).wait();
    const project = await creator
      .insert(app.projects, { title: "Revocable scope" })
      .wait({ tier: "global" });
    await creator.e2ee.spaces.grant(app.projects, project.id, alice.account.id).wait();
    await creator.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await removed.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await remaining.e2ee.explain(target)).toEqual({ state: "ready" });
    const root = (await creator.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "remote",
    }))!;
    // Reuse the first removal to prove an active former member cannot rotate,
    // even with the old key, a valid signature and ordinary Jazz write permission.
    const delivery = (
      await removed.all(
        app.__e2ee_space_deliveries.where({
          spaceId: root.id,
          recipientDeviceId: bobDevice!.id,
        }),
        { tier: "remote" },
      )
    )[0]!;
    // The adversary owns their host key store. Only the public context string is taken from Alice's store.
    const stored = JSON.parse((await bobStore.read())!).devices[0];
    const application = JSON.parse((await aliceStore.read())!).devices[0].scope as string;
    const device = {
      publicKey: Uint8Array.from(stored.publicKey),
      privateKey: Uint8Array.from(stored.privateKey),
    };
    const signingKey = Uint8Array.from(stored.signingPrivateKey);
    secrets.push(device.privateKey, signingKey);
    const oldKey = await keys.open(
      device,
      spaceDeliveryContext(application, root, delivery),
      delivery.envelope,
    );
    secrets.push(oldKey);
    const confirmation = await keys.unwrap(
      oldKey,
      spaceContext(application, root, "verification"),
      root.verification,
    );
    expect(confirmation.length === 32 && confirmation.every((byte) => byte === 0)).toBe(true);
    confirmation.fill(0);

    failNextWrap = true;
    const revoke = creator.e2ee.spaces.revoke(app.projects, project.id, bob.account.id);
    expect(revoke).not.toBeInstanceOf(Promise);
    await expect(revoke.wait()).rejects.toThrow("fixture: pause successor creation");
    expect(failNextWrap).toBe(false);
    const query = app.__e2ee_space_successors.where({ spaceId: root.id });
    expect(await creator.all(query, { tier: "remote" })).toEqual([]);
    const grants = await creator.all(app.__e2ee_space_grants.where({ spaceId: root.id }), {
      tier: "remote",
    });
    expect(
      grants.filter((row) => row.operation === "remove" && row.recipientId === bob.account.id),
    ).toHaveLength(1);
    expect(await removed.e2ee.explain(target)).toMatchObject({ state: "refused" });
    expect(
      (await removed.e2ee.devices.list()).find((row) => row.id === bobDevice!.id),
    ).toMatchObject({ state: "active" });
    const aliceRoot = (await creator.one(
      app.__e2ee_account_roots.where({ accountId: alice.account.id }),
      { tier: "remote" },
    ))!;
    const bobRoot = (await removed.one(
      app.__e2ee_account_roots.where({ accountId: bob.account.id }),
      { tier: "remote" },
    ))!;
    const nextKey = globalThis.crypto.getRandomValues(new Uint8Array(32));
    secrets.push(nextKey);
    const coordinates = {
      id: globalThis.crypto.randomUUID(),
      spaceId: root.id,
      predecessor: root.epochId,
      epochId: globalThis.crypto.randomUUID(),
      authorAccountId: bob.account.id,
      authorDeviceId: bobDevice!.id,
      authorEpochId: bobRoot.epochId,
    };
    const record = {
      ...coordinates,
      revision: encodePublicApprovalRevision(grants.map((row) => "space-grant:" + row.id)),
      membership: encodeGroupMembership(new Map([[alice.account.id, aliceRoot.epochId]])),
      verification: await keys.wrap(
        nextKey,
        spaceContext(application, { ...root, epochId: coordinates.epochId }, "verification"),
        new Uint8Array(32),
      ),
      history: await keys.wrap(
        nextKey,
        spaceSuccessorContext(application, root, coordinates, "history"),
        oldKey,
      ),
      authorEnvelope: await keys.seal(
        device.publicKey,
        spaceSuccessorContext(application, root, coordinates, "author-envelope", bobDevice!.id),
        nextKey,
      ),
    };
    const prior = await keys.unwrap(
      nextKey,
      spaceSuccessorContext(application, root, coordinates, "history"),
      record.history,
    );
    expect(prior.length === oldKey.length && prior.every((byte, i) => byte === oldKey[i])).toBe(
      true,
    );
    prior.fill(0);
    const recovered = await keys.open(
      device,
      spaceSuccessorContext(application, root, coordinates, "author-envelope", bobDevice!.id),
      record.authorEnvelope,
    );
    expect(
      recovered.length === nextKey.length && recovered.every((byte, i) => byte === nextKey[i]),
    ).toBe(true);
    recovered.fill(0);
    const bytes = spaceSuccessorBytes(application, root, record);
    const signature = await signer.sign(signingKey, bytes);
    expect(await signer.verify(Uint8Array.from(stored.signingPublicKey), bytes, signature)).toBe(
      true,
    );
    const { id, ...values } = record;
    // A real accepted Jazz write, not a fake injected snapshot.
    await removed
      .insert(app.__e2ee_space_successors, { ...values, signature }, { id })
      .wait({ tier: "global" });
    expect(await creator.all(query, { tier: "remote" })).toHaveLength(1);
    expect(await removed.e2ee.explain(target)).toMatchObject({ state: "refused" });
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await removed.e2ee.explain(target)).toMatchObject({ state: "refused" });
    expect(await remaining.e2ee.explain(target)).toEqual({ state: "ready" });
    const candidates = await creator.all(query, { tier: "remote" });
    expect(candidates).toHaveLength(2);
    const successors = candidates.filter((row) => row.id !== id);
    expect(successors).toHaveLength(1);
    const accepted = successors[0]!;
    // The forgery has the same predecessor, membership and revision as the
    // accepted rotation: its removed author is the reason it is ineligible.
    expect(accepted.predecessor).toBe(record.predecessor);
    expect(accepted.membership).toEqual(record.membership);
    expect(accepted.revision).toEqual(record.revision);
    expect(accepted.authorAccountId).toBe(alice.account.id);
    expect(
      await creator.all(
        app.__e2ee_space_deliveries.where({ spaceId: root.id, epochId: record.epochId }),
        { tier: "remote" },
      ),
    ).toEqual([]);
    expect(successors[0]!.predecessor).toBe(root.epochId);
    expect(successors[0]!.epochId).not.toBe(root.epochId);
    const deliveries = await creator.all(
      app.__e2ee_space_deliveries.where({ spaceId: root.id, epochId: successors[0]!.epochId }),
      { tier: "remote" },
    );
    expect(deliveries.map((row) => row.recipientDeviceId).sort()).toEqual(
      [first!.id, request.id].sort(),
    );
    expect(deliveries.some((row) => row.recipientDeviceId === bobDevice!.id)).toBe(false);
    expect(deliveries.every((row) => row.recipientAccountId === alice.account.id)).toBe(true);

    // Re-adding Bob shares the current history, but does not create a new epoch.
    await creator.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait();
    expect(await removed.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(
      await creator.all(app.__e2ee_space_successors.where({ spaceId: root.id }), {
        tier: "remote",
      }),
    ).toHaveLength(2);

    // A different remaining device authors the next rotation.
    await remaining.e2ee.spaces.revoke(app.projects, project.id, bob.account.id).wait();
    expect(await removed.e2ee.explain(target)).toMatchObject({
      state: "refused",
      reason: "not-a-space-recipient",
    });
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
    const allHistory = await creator.all(app.__e2ee_space_successors.where({ spaceId: root.id }), {
      tier: "remote",
    });
    expect(allHistory).toHaveLength(3);
    expect(allHistory.find((row) => row.id === id)).toEqual(
      candidates.find((row) => row.id === id),
    );
    const history = allHistory.filter((row) => row.id !== id);
    expect(history).toHaveLength(2);
    const latest = history.find((row) => row.predecessor === successors[0]!.epochId)!;
    expect(latest).toBeDefined();
    expect(latest.authorDeviceId).toBe(request.id);
    expect(new Set([root.epochId, ...history.map((row) => row.epochId)]).size).toBe(3);
    const latestDeliveries = await creator.all(
      app.__e2ee_space_deliveries.where({ spaceId: root.id, epochId: latest.epochId }),
      { tier: "remote" },
    );
    expect(latestDeliveries.map((row) => row.recipientDeviceId).sort()).toEqual(
      [first!.id, request.id].sort(),
    );

    // This device has never seen either old epoch. Its latest delivery must
    // suffice to validate the complete predecessor chain.
    const historyError = new Error("History provider unavailable");
    let failHistory = true;
    const latestHistoryContext = spaceSuccessorContext(application, root, latest, "history");
    const olderHistoryContext = spaceSuccessorContext(application, root, accepted, "history");
    const historyContexts: Uint8Array[] = [];
    const newcomer = await createDb({
      ...alice,
      e2ee: {
        app,
        store: store(),
        crypto: {
          ...crypto,
          keyEnvelope: {
            ...crypto.keyEnvelope,
            async unwrap(key, context, envelope) {
              if (failHistory && new TextDecoder().decode(context).includes("history")) {
                historyContexts.push(context.slice());
                if (
                  context.length === olderHistoryContext.length &&
                  context.every((byte, index) => byte === olderHistoryContext[index])
                )
                  throw historyError;
              }
              return crypto.keyEnvelope.unwrap(key, context, envelope);
            },
          },
        },
      },
    });
    clients.push(newcomer);
    const newRequest = (await newcomer.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await remaining.e2ee.devices.approve(newRequest.id).wait();
    expect(await remaining.e2ee.explain(target)).toEqual({ state: "ready" });
    // The non-author device must propagate a failure after opening one predecessor.
    await expect(newcomer.e2ee.explain(target)).rejects.toBe(historyError);
    expect(historyContexts).toEqual([latestHistoryContext, olderHistoryContext]);
    failHistory = false;
    expect(await newcomer.e2ee.explain(target)).toEqual({ state: "ready" });
    const newcomerDeliveries = await creator.all(
      app.__e2ee_space_deliveries.where({ spaceId: root.id, recipientDeviceId: newRequest.id }),
      { tier: "remote" },
    );
    expect(newcomerDeliveries.map((row) => row.epochId)).toEqual([latest.epochId]);
    expect(await removed.e2ee.explain(target)).toMatchObject({ state: "refused" });
  } finally {
    for (const secret of secrets) secret.fill(0);
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 180_000);
