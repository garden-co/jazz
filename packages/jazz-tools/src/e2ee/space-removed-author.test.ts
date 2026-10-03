import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserKeyEnvelope, createBrowserDeviceSigner } from "./browser.js";
import { spaceContext, spaceDeliveryContext } from "./space-format.js";
import { spaceSuccessorBytes, spaceSuccessorContext } from "./space-successor.js";
import { encodePublicApprovalRevision } from "./account-successor.js";
import { encodeGroupMembership } from "./group-successor.js";

it("ignores a removed account's validly signed successor despite retained old keys and Jazz write permission", async () => {
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
  const aliceStore = store(),
    bobStore = store();
  const keys = await createBrowserKeyEnvelope();
  const signer = await createBrowserDeviceSigner();
  let failNextWrap = false;
  const secrets: Uint8Array[] = [];
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
    const owner = await createDb({
      ...alice,
      e2ee: {
        app,
        store: aliceStore,
        crypto: {
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
    clients.push(owner, removed);
    await owner.e2ee.devices.list();
    const [bobDevice] = await removed.e2ee.devices.list();
    const project = await owner
      .insert(app.projects, { title: "Adversarial rotation" })
      .wait({ tier: "global" });
    await owner.e2ee.spaces.grant(app.projects, project.id, alice.account.id).wait();
    await owner.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await removed.e2ee.explain(target)).toEqual({ state: "ready" });
    const root = (await owner.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "edge",
    }))!;
    const delivery = (
      await removed.all(
        app.__e2ee_space_deliveries.where({
          spaceId: root.id,
          recipientDeviceId: bobDevice!.id,
        }),
        { tier: "edge" },
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
    await expect(
      owner.e2ee.spaces.revoke(app.projects, project.id, bob.account.id).wait(),
    ).rejects.toThrow("fixture: pause successor creation");
    expect(failNextWrap).toBe(false);
    const query = app.__e2ee_space_successors.where({ spaceId: root.id });
    expect(await owner.all(query, { tier: "edge" })).toEqual([]);
    const grants = await owner.all(app.__e2ee_space_grants.where({ spaceId: root.id }), {
      tier: "edge",
    });
    expect(
      grants.filter((row) => row.operation === "remove" && row.recipientId === bob.account.id),
    ).toHaveLength(1);
    expect(await removed.e2ee.explain(target)).toMatchObject({ state: "refused" });
    expect(
      (await removed.e2ee.devices.list()).find((row) => row.id === bobDevice!.id),
    ).toMatchObject({ state: "active" });
    const aliceRoot = (await owner.one(
      app.__e2ee_account_roots.where({ accountId: alice.account.id }),
      { tier: "edge" },
    ))!;
    const bobRoot = (await removed.one(
      app.__e2ee_account_roots.where({ accountId: bob.account.id }),
      { tier: "edge" },
    ))!;
    const nextKey = crypto.getRandomValues(new Uint8Array(32));
    secrets.push(nextKey);
    const coordinates = {
      id: crypto.randomUUID(),
      spaceId: root.id,
      predecessor: root.epochId,
      epochId: crypto.randomUUID(),
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
    expect(await owner.all(query, { tier: "edge" })).toHaveLength(1);
    expect(await owner.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await removed.e2ee.explain(target)).toMatchObject({ state: "refused" });
    const candidates = await owner.all(query, { tier: "edge" });
    expect(candidates).toHaveLength(2);
    const accepted = candidates.find((row) => row.id !== id)!;
    // The attack was not rejected because it named a stale predecessor or wrong membership/revision.
    expect(accepted.predecessor).toBe(record.predecessor);
    expect(accepted.membership).toEqual(record.membership);
    expect(accepted.revision).toEqual(record.revision);
    expect(accepted.authorAccountId).toBe(alice.account.id);
    const deliveries = await owner.all(app.__e2ee_space_deliveries.where({ spaceId: root.id }), {
      tier: "edge",
    });
    expect(deliveries.filter((row) => row.epochId === record.epochId)).toEqual([]);
    const replacement = deliveries.filter((row) => row.epochId === accepted.epochId);
    expect(replacement).toHaveLength(1);
    expect(replacement[0]!.recipientAccountId).toBe(alice.account.id);
  } finally {
    for (const secret of secrets) secret.fill(0);
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
