import { expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createJazzSession, type JazzClient } from "../backend/create-jazz-session.js";
import type { JazzSession } from "../session/state.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import type { Db } from "../runtime/db.js";
import { accountGeneratedHere, exportLocalFirstSecret } from "../accounts/enrollment.js";
import {
  app,
  founderPermissions,
  privateStore,
  transportGate,
} from "./fixtures/offline-founder-fixture.js";

it("refuses second-device and missing-journal founders without changing the durable owner", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const gate = await transportGate(server.url);
  const directory = await mkdtemp(join(tmpdir(), "jazz-single-founder-"));
  const accountStore = privateStore();
  const founderStore = privateStore();
  const secondStore = privateStore();
  const sessions: JazzSession<JazzClient>[] = [];
  const config = {
    appId: server.appId,
    app,
    permissions: founderPermissions,
    serverUrl: gate.url,
    env: "test",
  };
  const founderDriver = { type: "persistent" as const, dataPath: join(directory, "founder") };
  const secondDriver = { type: "persistent" as const, dataPath: join(directory, "second") };
  const missingJournalDriver = {
    type: "persistent" as const,
    dataPath: join(directory, "missing-journal"),
  };
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: founderPermissions,
    });
    // Authenticate each durable root's catalogue before cutting TCP. Warm-up
    // accounts are separate from the one whose first-device claim is tested.
    // An authenticated read warms the catalogue without creating an unused space.
    for (const driver of [founderDriver, secondDriver, missingJournalDriver]) {
      const warm = await createJazzSession({
        ...config,
        driver,
        initial: "local-first",
        store: privateStore(),
        e2ee: { app, store: privateStore() },
      });
      sessions.push(warm);
      await warm.getSnapshot().client!.db.all(app.projects, { tier: "remote" });
      await warm.close();
      sessions.pop();
    }
    gate.block();

    const ownerConfig = {
      ...config,
      driver: founderDriver,
      store: accountStore,
      e2ee: { app, store: founderStore },
    };
    const owner = await createJazzSession({ ...ownerConfig, initial: "local-first" });
    sessions.push(owner);
    const { account, client } = owner.getSnapshot();
    const founder = client!.db;
    const tx = founder.beginSerializableTransaction();
    const project = tx.insert(app.projects, { title: "Original offline founder" });
    const notes = Array.from({ length: 3 }, () =>
      tx.insert(app.notes, {
        projectId: project.id,
        body: "Only the original device may encrypt",
      }),
    );
    const note = notes[0]!;
    const commit = tx.commit();
    await commit.wait({ tier: "local" });
    const originalTxId = await commit.txId;
    // A later transaction borrows the same provisional root without claiming
    // that it is globally accepted. Exercise its batch recheck while offline.
    const existing = founder.beginSerializableTransaction();
    notes.push(
      ...Array.from({ length: 2 }, () =>
        existing.insert(app.notes, {
          projectId: project.id,
          body: "Same provisional scope",
        }),
      ),
    );
    await existing.commit().wait({ tier: "local" });
    const updates = founder.beginSerializableTransaction();
    for (const row of notes) {
      updates.update(app.notes, row.id, { body: `${row.body} updated offline` });
    }
    await updates.commit().wait({ tier: "local" });
    for (const row of notes) row.body += " updated offline";
    expect(await founder.one(app.notes.where({ id: note.id }), { tier: "local-first" })).toEqual(
      note,
    );
    const observe = async (db: Db) => ({
      projects: await db.all(app.projects, { tier: "local-first" }),
      notes: await db.all(app.notes, { tier: "local-first" }),
      identities: await db.all(app.__e2ee_account_identities, { tier: "local-first" }),
      roots: await db.all(app.__e2ee_account_roots, { tier: "local-first" }),
      spaces: await db.all(app.__e2ee_spaces, { tier: "local-first" }),
      grants: await db.all(app.__e2ee_space_grants, { tier: "local-first" }),
      deliveries: await db.all(app.__e2ee_space_deliveries, { tier: "local-first" }),
    });
    const originalRows = await observe(founder);
    const originalState = (await founderStore.read())!;
    const originalJournal = JSON.parse(originalState).initializationJournalV1;
    const founderEntry = originalJournal.find(
      (entry: { proposal: string }) => JSON.parse(entry.proposal).kind === "founder",
    );
    expect(founderEntry).toMatchObject({
      reservation: expect.any(String),
      local: true,
      outcome: "pending",
    });
    const proposal = JSON.parse(founderEntry.proposal);
    expect(originalRows.identities).toEqual([
      expect.objectContaining({
        id: account!.id,
        deviceId: proposal.deviceId,
        epochId: proposal.epochId,
      }),
    ]);
    expect(originalRows.roots).toEqual([
      expect.objectContaining({ id: proposal.rootId, epochId: proposal.epochId }),
    ]);
    expect(originalRows.projects).toEqual([project]);
    expect([...originalRows.notes].sort((a, b) => a.id.localeCompare(b.id))).toEqual(
      [...notes].sort((a, b) => a.id.localeCompare(b.id)),
    );

    // Each refused opener has its own fresh authenticated durable root.
    // Check the intact owner before removing its journal for the second case.
    for (const missingJournal of [false, true]) {
      if (missingJournal) {
        await owner.close();
        sessions.pop();
        // Keep valid original keypairs, but lose the journal in an otherwise
        // valid store. A fresh catalogue root has no local identity to mask
        // accidental regeneration of the already-bound founder epoch.
        await founderStore.update((current) => {
          const state = JSON.parse(current!);
          delete state.initializationJournalV1;
          return JSON.stringify(state);
        });
      }
      const attemptedStore = missingJournal ? founderStore : secondStore;
      const attempt = (async () => {
        const other = await createJazzSession({
          ...config,
          driver: missingJournal ? missingJournalDriver : secondDriver,
          store: accountStore,
          e2ee: { app, store: attemptedStore },
        });
        sessions.push(other);
      })();
      if (missingJournal) {
        await expect(attempt).rejects.toMatchObject({
          code: "e2ee_initialization_not_ready",
          retryable: true,
        });
      } else {
        // A different device falls back to authority-governed enrolment. The
        // partition may fail that transport; it must not create another local
        // founder or pretend that remote enrolment completed.
        await expect(attempt).rejects.toThrow();
      }
      expect(
        JSON.parse((await attemptedStore.read()) ?? "{}").initializationJournalV1 ?? [],
      ).toEqual([]);

      let original = founder;
      if (missingJournal) {
        await founderStore.update(() => originalState);
        const reopened = await createJazzSession(ownerConfig);
        sessions.push(reopened);
        expect(reopened.getSnapshot().account!.id).toBe(account!.id);
        original = reopened.getSnapshot().client!.db;
      }
      expect(await observe(original)).toEqual(originalRows);
      expect(JSON.parse((await founderStore.read())!).initializationJournalV1).toEqual(
        originalJournal,
      );
      expect(await commit.txId).toBe(originalTxId);
    }
  } finally {
    await Promise.all(sessions.map((session) => session.close()));
    await gate.close();
    await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
}, 60_000);

it("resumes the exact pending founder journal after importing into the same durable root", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const gate = await transportGate(server.url);
  const directory = await mkdtemp(join(tmpdir(), "jazz-imported-founder-"));
  const store = privateStore();
  const config = {
    appId: server.appId,
    app,
    permissions: founderPermissions,
    serverUrl: gate.url,
    env: "test",
    driver: { type: "persistent" as const, dataPath: join(directory, "database") },
    e2ee: { app, store },
  };
  let owner: JazzSession<JazzClient> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: founderPermissions,
    });
    // The catalogue is shared by this scoped persistent root, not by a live Db.
    owner = await createJazzSession({
      ...config,
      initial: "local-first",
      store: privateStore(),
      e2ee: { app, store: privateStore() },
    });
    await owner.getSnapshot().client!.db.all(app.projects, { tier: "remote" });
    await owner.close();
    owner = undefined;
    gate.block();

    owner = await createJazzSession({
      ...config,
      initial: "local-first",
      store: privateStore(),
    });
    const { account, client } = owner.getSnapshot();
    expect(await accountGeneratedHere(account!)).toBe(true);
    const founder = client!.db;
    const tx = founder.beginSerializableTransaction();
    const project = tx.insert(app.projects, { title: "Pending founder space" });
    const note = tx.insert(app.notes, {
      projectId: project.id,
      body: "Original offline message",
    });
    await tx.commit().wait({ tier: "local" });
    expect(await founder.one(app.notes.where({ id: note.id }), { tier: "local-first" })).toEqual(
      note,
    );
    const root = await founder.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "local-first",
    });
    expect(root?.creatorAccountId).toBe(account!.id);
    const journal = JSON.parse((await store.read())!).initializationJournalV1;
    expect(
      journal.find((entry: { proposal: string }) => JSON.parse(entry.proposal).kind === "founder"),
    ).toMatchObject({ local: true, outcome: "pending" });
    const secret = exportLocalFirstSecret(account!);
    await owner.close();
    owner = undefined;

    // A fresh account store proves imported provenance; only the durable root
    // and separate E2EE store retain the original owner-bound transactions.
    owner = await createJazzSession({ ...config, store: privateStore() });
    expect(owner.getSnapshot().status).toBe("signed-out");
    await owner.restoreLocalFirst(secret);
    const restored = owner.getSnapshot();
    expect(restored.account!.id).toBe(account!.id);
    expect(await accountGeneratedHere(restored.account!)).toBe(false);
    const reopened = restored.client!.db;
    expect(
      await reopened.one(app.projects.where({ id: project.id }), { tier: "local-first" }),
    ).toEqual(project);
    expect(await reopened.one(app.notes.where({ id: note.id }), { tier: "local-first" })).toEqual(
      note,
    );
    expect(
      await reopened.one(app.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "local-first",
      }),
    ).toEqual(root);
    expect(JSON.parse((await store.read())!).initializationJournalV1).toEqual(journal);
  } finally {
    await owner?.close();
    await gate.close();
    await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
}, 60_000);

it("never turns a provisional author's sealed envelope into recipient membership", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  try {
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.uploads.allowRead.always();
      policy.uploads.allowInsert.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ creatorAccountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const ownerAccount = await localAccountConfig(server.appId, server.url);
    const recipientAccount = await localAccountConfig(server.appId, server.url);
    const owner = await createDb({ ...ownerAccount, e2ee: { app, store: privateStore() } });
    const recipient = await createDb({ ...recipientAccount, e2ee: { app, store: privateStore() } });
    clients.push(owner, recipient);
    await Promise.all([owner.e2ee.devices.list(), recipient.e2ee.devices.list()]);
    await owner
      .insert(
        app.projects,
        { title: "Known accepted recipients" },
        {
          initialRecipients: [ownerAccount.account.id, recipientAccount.account.id],
        },
      )
      .wait({ tier: "global" });
    await owner.disconnect();
    const tx = owner.beginSerializableTransaction();
    const project = tx.insert(
      app.projects,
      { title: "Recipient only" },
      { initialRecipients: [recipientAccount.account.id] },
    );
    const note = tx.insert(app.notes, {
      projectId: project.id,
      body: "Only the selected recipient",
    });
    const pending = tx.commit();
    await pending.wait({ tier: "local" });
    await expect(
      owner.one(app.notes.where({ id: note.id }), { tier: "local-first" }),
    ).rejects.toMatchObject({ code: "key-not-shared" });
    await expect(
      owner
        .insert(app.notes, { projectId: project.id, body: "No implicit creator grant" })
        .wait({ tier: "local" }),
    ).rejects.toMatchObject({ code: "key-not-shared" });
    const root = await owner.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "local-first",
    });
    const grants = await owner.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), {
      tier: "local-first",
    });
    expect(grants.map((grant) => grant.recipientId)).toEqual([recipientAccount.account.id]);
    await owner.reconnect();
    await pending.wait({ tier: "global" });
    expect(await recipient.one(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual(note);
    await expect(
      owner.one(app.notes.where({ id: note.id }), { tier: "remote" }),
    ).rejects.toMatchObject({ code: "key-not-shared" });
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
