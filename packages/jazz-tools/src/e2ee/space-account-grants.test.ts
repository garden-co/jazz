import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { PersistedWriteRejectedError, type TxId } from "../runtime/client.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createNativeCrypto } from "./native.js";

it("shares a space only after an authorised account grant, excluding pending devices", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const alice = await localAccountConfig(server.appId, server.url);
  const bob = await localAccountConfig(server.appId, server.url);
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
  });
  const policies = definePermissions(app, ({ policy, session, allOf }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where(() =>
      allOf([{ authorAccountId: session.user.account }, { authorAccountId: alice.account.id }]),
    );
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
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
      },
    });
    expect(alice.account.id).not.toBe(bob.account.id);
    const crypto = await createNativeCrypto();
    let failureMode: "reject" | "throw" | undefined;
    let deliveryFailures = 0;
    let removeDuringConfirmation: (() => Promise<void>) | undefined;
    let confirmations = 0;
    const openedDuringRemoval: Uint8Array[] = [];
    const deliveryError = new Error("Space delivery provider unavailable");
    let creatorOpeningError: Error | undefined;
    const creator = await createDb({
      ...alice,
      e2ee: {
        app,
        store: store(),
        crypto: {
          ...crypto,
          keyEnvelope: {
            ...crypto.keyEnvelope,
            open(pair, context, envelope) {
              const text = new TextDecoder().decode(context);
              if (
                creatorOpeningError &&
                text.includes("jazz.e2ee.space.v1") &&
                text.includes("author")
              ) {
                const error = creatorOpeningError;
                creatorOpeningError = undefined;
                throw error;
              }
              return crypto.keyEnvelope.open(pair, context, envelope);
            },
          },
        },
      },
    });
    const recipient = await createDb({
      ...bob,
      e2ee: {
        app,
        store: store(),
        crypto: {
          ...crypto,
          keyEnvelope: {
            ...crypto.keyEnvelope,
            async unwrap(secret, context, envelope) {
              const opened = await crypto.keyEnvelope.unwrap(secret, context, envelope);
              if (
                removeDuringConfirmation &&
                new TextDecoder().decode(context).includes("jazz.e2ee.space.v1")
              ) {
                confirmations++;
                // Interrupt delivery confirmation after the key has opened.
                // Its authority transaction must reject even with no writes.
                if (confirmations === 2) {
                  const remove = removeDuringConfirmation;
                  removeDuringConfirmation = undefined;
                  await remove();
                }
              }
              return opened;
            },
            // Non-async deliberately: exercise invocation throws as well as rejections.
            open(pair, context, envelope) {
              const text = new TextDecoder().decode(context);
              if (failureMode && text.includes("jazz.e2ee.space.v1") && text.includes("delivery")) {
                deliveryFailures++;
                if (failureMode === "throw") throw deliveryError;
                return Promise.reject(deliveryError);
              }
              const result = crypto.keyEnvelope.open(pair, context, envelope);
              if (removeDuringConfirmation)
                return result.then((opened) => {
                  openedDuringRemoval.push(opened);
                  return opened;
                });
              return result;
            },
          },
        },
      },
    });
    clients.push(creator, recipient);
    await creator.e2ee.devices.list();
    await recipient.e2ee.devices.list();
    const project = await creator
      .insert(app.projects, { title: "Shared scope" })
      .wait({ tier: "global" });
    await creator.e2ee.spaces.grant(app.projects, project.id, alice.account.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await recipient.e2ee.explain(target)).toMatchObject({ state: "refused" });
    await expect(
      recipient.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait(),
    ).rejects.toThrow();
    expect(await recipient.e2ee.explain(target)).toMatchObject({ state: "refused" });
    const unapproved = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(unapproved);
    const pending = (await unapproved.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    );
    expect(pending).toBeDefined();
    const grant = creator.e2ee.spaces.grant(app.projects, project.id, bob.account.id);
    expect(grant).not.toBeInstanceOf(Promise);
    await grant.wait();
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
    // An adapter's one-off SDK-shaped error must surface unchanged; treating
    // it as delivery denial would retry inspection and silently report ready.
    for (const code of ["permission_denied", "exclusive_conflict"]) {
      const inspectionError = new PersistedWriteRejectedError(
        "provider-inspection" as TxId,
        code,
        "Synthetic key provider failure",
      );
      creatorOpeningError = inspectionError;
      await expect(creator.e2ee.explain(target)).rejects.toBe(inspectionError);
      expect(creatorOpeningError).toBeUndefined();
    }
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
    // Both provider failure shapes use the same authorised delivery. Prove each
    // still reaches the provider, including after the preceding successful retry.
    for (const mode of ["reject", "throw"] as const) {
      const before = deliveryFailures;
      failureMode = mode;
      expect(await recipient.e2ee.explain(target)).toEqual({
        state: "unavailable",
        reason: "space-key-not-delivered",
      });
      expect(deliveryFailures).toBeGreaterThan(before);
      failureMode = undefined;
      expect(await recipient.e2ee.explain(target)).toEqual({ state: "ready" });
    }
    expect(await unapproved.e2ee.explain(target)).toMatchObject({ state: "refused" });
    expect(
      await creator.all(app.__e2ee_space_deliveries.where({ recipientDeviceId: pending!.id }), {
        tier: "remote",
      }),
    ).toEqual([]);
    const beforeRemoval = await creator.all(app.__e2ee_space_deliveries, { tier: "global" });
    let removed = false;
    removeDuringConfirmation = async () => {
      await creator.e2ee.spaces.revoke(app.projects, project.id, bob.account.id).wait();
      removed = true;
    };
    expect(await recipient.e2ee.explain(target)).toEqual({
      state: "refused",
      reason: "not-a-space-recipient",
    });
    expect(confirmations).toBe(2);
    expect(removed).toBe(true);
    expect(openedDuringRemoval.length).toBeGreaterThan(0);
    expect(openedDuringRemoval.every((secret) => secret.every((byte) => byte === 0))).toBe(true);
    expect(await creator.all(app.__e2ee_space_deliveries, { tier: "global" })).toEqual(
      beforeRemoval,
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
