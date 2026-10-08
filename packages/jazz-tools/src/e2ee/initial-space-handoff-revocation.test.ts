import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createNativeCrypto } from "./native.js";

it("rejects initial delivery if its author is revoked while opening the accepted key", async () => {
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
  });
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
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  let beforeOpenReturns: ((envelope: Uint8Array) => Promise<boolean>) | undefined;
  let openedSecret: Uint8Array | undefined;
  let revocationAccepted = false;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const account = await localAccountConfig(server.appId, server.url);
    const native = await createNativeCrypto();
    const creator = await createDb({
      ...account,
      e2ee: {
        app,
        store: store(),
        crypto: {
          ...native,
          keyEnvelope: {
            ...native.keyEnvelope,
            async open(pair, context, envelope) {
              const secret = await native.keyEnvelope.open(pair, context, envelope);
              const text = new TextDecoder().decode(context);
              if (
                beforeOpenReturns &&
                text.includes("jazz.e2ee.space.v1") &&
                text.includes("author")
              ) {
                try {
                  if (await beforeOpenReturns(envelope)) {
                    beforeOpenReturns = undefined;
                    openedSecret = secret;
                    revocationAccepted = true;
                  }
                } catch (error) {
                  secret.fill(0);
                  throw error;
                }
              }
              return secret;
            },
          },
        },
      },
    });
    clients.push(creator);
    const author = (await creator.e2ee.devices.list()).find((device) => device.state === "active");
    expect(author).toBeDefined();
    const other = await createDb({ ...account, e2ee: { app, store: store() } });
    clients.push(other);
    const pending = (await other.e2ee.devices.list()).find((device) => device.state === "pending");
    expect(pending).toBeDefined();
    await creator.e2ee.devices.approve(pending!.id).wait();
    expect(await other.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending!.id, state: "active" }),
    );
    const project = await creator
      .insert(app.projects, { title: "Author revoked during handoff" })
      .wait({ tier: "global" });
    beforeOpenReturns = async (envelope) => {
      const accepted = await other.one(app.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "global",
      });
      // Initial preparation also opens its envelope to verify the crypto adapter.
      // Target the later handoff, whose original root is already accepted.
      if (!accepted) return false;
      expect(accepted.authorEnvelope).toEqual(envelope);
      await other.e2ee.devices.revoke(author!.id).wait();
      return true;
    };
    await expect(
      creator.e2ee.spaces.grant(app.projects, project.id, account.account.id).wait(),
    ).rejects.toThrow();
    expect(revocationAccepted).toBe(true);
    expect(openedSecret).toHaveLength(32);
    expect(openedSecret!.every((byte) => byte === 0)).toBe(true);
    // Revocation does not undo the already accepted initial root or grant, but
    // no envelope prepared with the old authority snapshot may be accepted.
    const root = await other.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "global",
    });
    expect(root).toMatchObject({ deviceId: author!.id });
    expect(
      await other.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), { tier: "global" }),
    ).toHaveLength(1);
    expect(
      await other.all(app.__e2ee_space_deliveries.where({ spaceId: root!.id }), { tier: "global" }),
    ).toEqual([]);
    expect(await other.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: author!.id, state: "revoked" }),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
