import { expect, it } from "vitest";
import {
  createAccountManager,
  createDb,
  definePermissions,
  schema as s,
  type AccountStore,
  type Db,
} from "jazz-tools";
import { deploy } from "../../src/dev/catalogue.js";
import { withTimeout } from "./support.js";
import {
  blockJazzServerNetwork,
  getJazzServerInfo,
  stopJazzServer,
  unblockJazzServerNetwork,
} from "./testing-server.js";

// Keep both account provenance and private E2EE material outside the Db lifetime.
// No in-memory fallback: this fixture exercises the browser's durable store.
function browserStore(key: string): AccountStore {
  return {
    async read() {
      return localStorage.getItem(key);
    },
    async update(transform) {
      await navigator.locks.request(key, () => {
        localStorage.setItem(key, transform(localStorage.getItem(key)));
      });
    },
  };
}

it("automatically creates and reopens a fresh founder's encrypted text and image offline", async () => {
  const server = await getJazzServerInfo(`e2ee-offline-founder-${crypto.randomUUID()}`, true);
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
    images: s
      .table(
        { projectId: s.uuid(), name: s.string(), payload: s.bytes() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["name", "payload"] }),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.notes.allowRead.always();
    policy.notes.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.notes.allowUpdate.always();
    policy.images.allowRead.always();
    policy.images.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
  });
  const keys = ["accounts", "online-device", "founder-device"].map(
    (kind) => `e2ee-offline-founder:${server.appId}:${kind}`,
  );
  const managerConfig = {
    appId: server.appId,
    serverUrl: server.serverUrl,
    store: browserStore(keys[0]!),
  };
  let db: Db | undefined;
  let recipient: Db | undefined;
  let blocked = false;
  let scenarioError: unknown;
  try {
    await deploy({ ...server, schema: app, permissions });
    const accounts = await createAccountManager(managerConfig);
    const onlineAccount = accounts.createLocalFirst();
    db = await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      account: onlineAccount,
      driver: { type: "persistent", dbName: `catalogue-witness-${server.appId}` },
      e2ee: { app, store: browserStore(keys[1]!) },
    });
    // Learn the authority's catalogue through ordinary authenticated sync under
    // another account, not a fabricated cache or copied account database.
    await db.insert(app.projects, { title: "Online catalogue witness" }).wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await blockJazzServerNetwork(server.serverUrl);
    blocked = true;

    const founder = accounts.createLocalFirst();
    expect(founder.id).not.toBe(onlineAccount.id);
    const config = {
      appId: server.appId,
      serverUrl: server.serverUrl,
      account: founder,
      driver: { type: "persistent" as const, dbName: `offline-founder-${server.appId}` },
      e2ee: { app, store: browserStore(keys[2]!) },
    };
    // No explicit offline option, disconnect, device enrolment or space setup.
    db = await withTimeout(
      createDb(config),
      10_000,
      "Fresh founder opening requires the network",
    ).catch((cause) => {
      throw new Error(
        "Fresh offline founder could not open after authenticated catalogue warm-up",
        {
          cause,
        },
      );
    });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(
      app.projects,
      { title: "Created entirely offline" },
      { initialRecipients: [founder.id] },
    );
    const note = tx.insert(app.notes, {
      projectId: project.id,
      body: "Offline from the first write",
    });
    await withTimeout(
      tx.commit().wait({ tier: "local" }),
      10_000,
      "Fresh founder encrypted text did not become locally durable without authority",
    );
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual(note);
    const updatedNote = { ...note, body: "Updated before the founder reached authority" };
    await withTimeout(
      db.update(app.notes, note.id, { body: updatedNote.body }).wait({ tier: "local" }),
      10_000,
      "Ordinary provisional encrypted update required manual transaction setup",
    );
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual(updatedNote);

    // A real one-pixel PNG, delivered in chunks through the public upload seam.
    const imageBytes = Uint8Array.from(
      atob(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jGZkAAAAASUVORK5CYII=",
      ),
      (byte) => byte.charCodeAt(0),
    );
    let sourceReads = 0;
    let planCalls = 0;
    const upload = await withTimeout(
      db.streamingTransaction((plan) => {
        planCalls++;
        return plan.insertStreaming(app.images, {
          projectId: project.id,
          name: "offline.png",
          payload: (async function* () {
            sourceReads++;
            for (let offset = 0; offset < imageBytes.length; offset += 17) {
              yield imageBytes.slice(offset, offset + 17);
            }
          })(),
        });
      }),
      10_000,
      "Fresh founder image preparation requires the network",
    );
    const image = await withTimeout(
      upload.wait({ tier: "local" }),
      10_000,
      "Offline image durability stalled",
    );
    const expectedImage = {
      ...image,
      projectId: project.id,
      name: "offline.png",
      payload: imageBytes,
    };
    expect(await db.one(app.images.where({ id: image.id }), { tier: "local" })).toEqual(
      expectedImage,
    );
    const roots = await db.all(app.__e2ee_spaces, { tier: "local" });
    const grants = await db.all(app.__e2ee_space_grants, { tier: "local" });
    expect(roots).toEqual([
      expect.objectContaining({ accountId: founder.id, identifier: project.id }),
    ]);
    expect(grants).toEqual([expect.objectContaining({ recipientId: founder.id })]);
    expect({ planCalls, sourceReads }).toEqual({ planCalls: 1, sourceReads: 1 });
    await withTimeout(db.shutdown(), 10_000, "Offline founder shutdown stalled");
    db = undefined;

    const reopenedAccounts = await createAccountManager(managerConfig);
    const retainedFounder = reopenedAccounts.getLoggedIn();
    if (!retainedFounder) throw new Error("Generated-here founder was not retained across restart");
    expect(retainedFounder.id).toBe(founder.id);
    db = await withTimeout(
      createDb({
        ...config,
        account: retainedFounder,
        e2ee: { app, store: browserStore(keys[2]!) },
      }),
      10_000,
      "Offline founder reopening requires the network",
    );
    // Exact scope/grant records must survive, rather than a replacement epoch.
    expect(await db.all(app.projects, { tier: "local" })).toEqual([project]);
    expect(await db.all(app.notes, { tier: "local" })).toEqual([updatedNote]);
    expect(await db.all(app.images, { tier: "local" })).toEqual([expectedImage]);
    expect(await db.all(app.__e2ee_spaces, { tier: "local" })).toEqual(roots);
    expect(await db.all(app.__e2ee_space_grants, { tier: "local" })).toEqual(grants);
    expect({ planCalls, sourceReads }).toEqual({ planCalls: 1, sourceReads: 1 });

    await unblockJazzServerNetwork(server.serverUrl);
    blocked = false;
    await withTimeout(db.reconnect(), 10_000, "Founder reconnect stalled");
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual(updatedNote);
    expect(await db.one(app.images.where({ id: image.id }), { tier: "global" })).toEqual(
      expectedImage,
    );
    expect(
      await db.all(app.__e2ee_spaces.where({ identifier: project.id }), { tier: "global" }),
    ).toEqual(roots);
    recipient = await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      account: onlineAccount,
      driver: { type: "persistent", dbName: `catalogue-witness-${server.appId}` },
      e2ee: { app, store: browserStore(keys[1]!) },
    });
    await withTimeout(
      db.e2ee.spaces.grant(app.projects, project.id, onlineAccount.id).wait(),
      60_000,
      "Accepted founder could not deliver the original image key",
    );
    expect(
      await withTimeout(
        recipient.one(app.images.where({ id: image.id }), { tier: "global" }),
        30_000,
        "Recipient encrypted image unavailable",
      ),
    ).toEqual(expectedImage);
    expect({ planCalls, sourceReads }).toEqual({ planCalls: 1, sourceReads: 1 });
  } catch (error) {
    scenarioError = error;
    throw error;
  } finally {
    const cleanupErrors: unknown[] = [];
    try {
      if (blocked) await unblockJazzServerNetwork(server.serverUrl);
      const closures = await Promise.allSettled(
        [db, recipient].map((client) =>
          client
            ? withTimeout(client.shutdown(), 5_000, "Founder test cleanup stalled")
            : undefined,
        ),
      );
      for (const result of closures)
        if (result.status === "rejected") cleanupErrors.push(result.reason);
    } catch (error) {
      cleanupErrors.push(error);
    }
    try {
      await withTimeout(stopJazzServer(server.serverUrl), 5_000, "Server cleanup stalled");
    } catch (error) {
      cleanupErrors.push(error);
    }
    for (const key of keys) localStorage.removeItem(key);
    if (cleanupErrors.length)
      throw new AggregateError(
        scenarioError === undefined ? cleanupErrors : [scenarioError, ...cleanupErrors],
        "Offline founder scenario or cleanup failed",
      );
  }
}, 120_000);
