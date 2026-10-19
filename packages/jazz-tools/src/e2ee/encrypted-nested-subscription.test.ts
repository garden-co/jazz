import { afterAll, beforeAll, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserCrypto } from "./browser.js";

async function createFixture() {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    folders: s.table(
      { title: s.string() },
      { notesViaFolder: s.reverse("notes", "folderRelation") },
    ),
    notes: s
      .table(
        { projectId: s.uuid(), folder: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId"), folderRelation: s.rel("folders", "folder") },
      )
      .encrypted({ space: "projectId", columns: ["title"] }),
  });
  const policies = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.folders.allowRead.always();
    policy.folders.allowInsert.always();
    policy.folders.allowUpdate.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowDelete.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ creatorAccountId: session.user.account });
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
  let validationGate: Promise<void> | undefined;
  let releaseValidation = () => {};
  let validationStarted = false;
  const release = () => {
    releaseValidation();
    validationGate = undefined;
  };
  const close = async () => {
    release();
    for (const client of clients) await client.shutdown();
    await server.stop();
  };
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const owner = await createDb({ ...alice, e2ee: { app, store: store() } });
    clients.push(owner);
    const crypto = await createBrowserCrypto();
    const reader = await createDb({
      ...bob,
      e2ee: {
        app,
        store: store(),
        crypto: {
          ...crypto,
          deviceSigner: {
            ...crypto.deviceSigner,
            async verify(...args) {
              if (validationGate) {
                validationStarted = true;
                await validationGate;
              }
              return crypto.deviceSigner.verify(...args);
            },
          },
        },
      },
    });
    clients.push(reader);
    await owner.e2ee.devices.list();
    await reader.e2ee.devices.list();
    return {
      app,
      server,
      bob,
      owner,
      reader,
      clients,
      close,
      holdValidation() {
        validationStarted = false;
        validationGate = new Promise<void>((resolve) => {
          releaseValidation = resolve;
        });
      },
      validationStarted: () => validationStarted,
      releaseValidation: release,
    };
  } catch (error) {
    await close();
    throw error;
  }
}

let fixture: Awaited<ReturnType<typeof createFixture>>;
beforeAll(async () => {
  fixture = await createFixture();
}, 120_000);
afterAll(async () => {
  await fixture?.close();
});

// Share only accepted account enrolment. Each case creates its own space,
// ciphertext and subscription, then removes that space's recipient grant.
it.each(["encrypted-space-revocation", "superseded-history-failure"])(
  "maintains included-result key dependencies (%s)",
  async (scenario) => {
    const superseded = scenario === "superseded-history-failure";
    const { app, server, bob, owner, reader, clients } = fixture;
    let stopPlaintext: (() => void) | undefined;
    try {
      const tx = owner.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title: "Private project" });
      const folder = tx.insert(app.folders, { title: "Shared folder" });
      const note = tx.insert(app.notes, {
        projectId: project.id,
        folder: folder.id,
        title: "Secret",
      });
      await tx.commit().wait({ tier: "global" });
      await owner.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait();
      if (!superseded) {
        // This observer has neither E2EE configuration nor a grant. Plaintext
        // includes must work independently of the encrypted reader's keys.
        const plainReader = await createDb(await localAccountConfig(server.appId, server.url));
        clients.push(plainReader);
        const plainQuery = app.folders.where({ id: folder.id }).include({
          notesViaFolder: app.notes.select("id"),
        });
        const plainExpected = [{ ...folder, notesViaFolder: [{ id: note.id }] }];
        expect(await plainReader.all(plainQuery, { tier: "remote" })).toEqual(plainExpected);
        let plainResult: unknown;
        let plainFailure: Error | undefined;
        const stop = plainReader.subscribe(
          plainQuery,
          {
            onUpdate: (rows) => {
              plainResult = rows;
            },
            onError: (error) => {
              plainFailure = error;
            },
          },
          { tier: "remote" },
        );
        try {
          await expect
            .poll(() => plainFailure ?? plainResult, { timeout: 30_000 })
            .toEqual(plainExpected);
          expect(plainFailure).toBeUndefined();
        } finally {
          stop();
        }
      }
      const query = app.folders.where({ id: folder.id }).include({
        notesViaFolder: true,
      });
      const expected = [{ ...folder, notesViaFolder: [note] }];
      expect(await reader.all(query, { tier: "remote" })).toEqual(expected);
      const snapshots: unknown[][] = [];
      let failure: Error | undefined;
      const stop = reader.subscribe(
        query,
        {
          onUpdate: (rows) => snapshots.push(rows),
          onError: (error) => {
            failure = error;
          },
        },
        { tier: "remote" },
      );
      try {
        await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 }).toEqual(expected);
        let visibleIds: unknown[] | undefined;
        if (superseded) {
          stopPlaintext = reader.subscribe(
            app.notes.where({ id: note.id }).select("id"),
            (rows) => {
              visibleIds = rows;
            },
            { tier: "remote" },
          );
          await expect.poll(() => visibleIds, { timeout: 10_000 }).toEqual([{ id: note.id }]);
          fixture.holdValidation();
        }
        await owner.e2ee.spaces.revoke(app.projects, project.id, bob.account.id).wait();
        if (superseded) {
          await expect.poll(() => fixture.validationStarted(), { timeout: 10_000 }).toBe(true);
          await owner.delete(app.notes, note.id).wait({ tier: "global" });
          // An ordinary subscription observes the removal while history verification is held.
          await expect.poll(() => visibleIds, { timeout: 10_000 }).toEqual([]);
          fixture.releaseValidation();
          await expect
            .poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 })
            .toEqual([{ ...folder, notesViaFolder: [] }]);
          await owner
            .update(app.folders, folder.id, { title: "Still readable" })
            .wait({ tier: "global" });
          await expect
            .poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 })
            .toEqual([{ ...folder, title: "Still readable", notesViaFolder: [] }]);
          expect(failure).toBeUndefined();
          return;
        }
        await expect.poll(() => failure?.name, { timeout: 10_000 }).toBe("E2eeDataError");
        expect(snapshots.every((rows) => JSON.stringify(rows) === JSON.stringify(expected))).toBe(
          true,
        );
      } finally {
        stop();
      }
    } finally {
      fixture.releaseValidation();
      stopPlaintext?.();
    }
  },
  120_000,
);
