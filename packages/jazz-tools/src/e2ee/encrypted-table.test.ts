import { expect, it } from "vitest";
import { Buffer } from "node:buffer";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createBrowserCrypto } from "./browser.js";
import { encodeEnvelope } from "./envelope.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";

it.each([
  "immediate",
  "after-yield",
  "plaintext-first",
  "transaction-read",
  "projection",
  "nested-results",
  "nested-projection",
  "nested-reference",
  "equality-index",
  "equality-collision",
  "equality-negative-zero",
  "equality-array-zero",
  "equality-enum-zero",
  "equality-bigint",
  "equality-null",
  "equality-json",
  "equality-owned-bytes",
  "equality-membership-invalidation",
  "equality-device-invalidation",
  "equality-device-invalidation-recipient",
  "equality-nonfinite",
  "equality-omitted-optional",
  "equality-composite-literals",
  "equality-transaction-initial",
  "equality-transaction-existing",
  "equality-subscription",
  "equality-subscription-collision",
  "equality-subscription-epoch",
  "equality-epoch-race",
  "transaction-projection",
  "subscription-projection",
  "subscription",
  "subscription-error",
  "offline-at-submit",
  "byoc-buffer",
  "ordinary-insert",
  "ordinary-insert-error",
  "ordinary-update",
  "ordinary-upsert",
  "ordinary-restore",
  "ordinary-scope-upsert",
  "transaction-scope-upsert",
])(
  "atomically creates a creator-owned space and encrypted data through ordinary mutations (%s)",
  async (timing) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table(
        { title: s.string() },
        {
          linkedNotesViaProject: s.reverse("linkedNotes", "projectRelation"),
          notesViaProject: s.reverse("notes", "project"),
        },
      ),
      counters: s
        .table(
          { projectId: s.uuid(), value: s.bigint() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["value"], indexes: { value: "equality" } }),
      events: s.table({ message: s.string() }, {}),
      publicDocuments: s.table(
        { projectId: s.uuid(), label: s.string(), body: s.json() },
        { project: s.rel("projects", "projectId") },
      ),
      privateDocuments: s
        .table(
          { projectId: s.uuid(), label: s.string(), body: s.json() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["body"], indexes: { body: "equality" } }),
      optionalNotes: s
        .table(
          { projectId: s.uuid(), title: s.string().optional() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
      optionalMeasurements: s
        .table(
          { projectId: s.uuid(), value: s.float().optional() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["value"], indexes: { value: "equality" } }),
      observations: s
        .table(
          {
            projectId: s.uuid(),
            numbers: s.array(s.bigint()),
            tagged: s.enum({ count: { value: s.bigint() } }),
            time: s.timestamp(),
          },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({
          space: "projectId",
          columns: ["numbers", "tagged", "time"],
          indexes: { numbers: "equality", tagged: "equality", time: "equality" },
        }),
      readings: s
        .table(
          {
            projectId: s.uuid(),
            reading: s.enum({ measured: { value: s.float() } }),
          },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["reading"], indexes: { reading: "equality" } }),
      samples: s
        .table(
          { projectId: s.uuid(), values: s.array(s.float()) },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["values"], indexes: { values: "equality" } }),
      measurements: s
        .table(
          { projectId: s.uuid(), value: s.float() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["value"], indexes: { value: "equality" } }),
      linkedNotes: s
        .table(
          { project: s.uuid(), title: s.string() },
          { projectRelation: s.rel("projects", "project") },
        )
        .encrypted({ space: "project", columns: ["title"] }),
      notes: s
        .table(
          {
            projectId: s.uuid(),
            title: s.string(),
            done: s.boolean(),
            payload: s.bytes(),
          },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({
          space: "projectId",
          columns: ["title", "payload"],
          ...(timing.startsWith("equality-") ? { indexes: { title: "equality" as const } } : {}),
          ...(timing === "equality-owned-bytes"
            ? { indexes: { payload: "equality" as const } }
            : {}),
        }),
    });
    const policies = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.events.allowRead.always();
      policy.events.allowInsert.always();
      policy.publicDocuments.allowRead.always();
      policy.publicDocuments.allowInsert.always();
      policy.privateDocuments.allowRead.always();
      policy.privateDocuments.allowInsert.always();
      policy.optionalNotes.allowRead.always();
      policy.optionalNotes.allowInsert.always();
      policy.optionalMeasurements.allowRead.always();
      policy.optionalMeasurements.allowInsert.always();
      policy.measurements.allowRead.always();
      policy.observations.allowRead.always();
      policy.observations.allowInsert.always();
      policy.counters.allowRead.always();
      policy.counters.allowInsert.always();
      policy.measurements.allowInsert.always();
      policy.samples.allowRead.always();
      policy.samples.allowInsert.always();
      policy.readings.allowRead.always();
      policy.readings.allowInsert.always();
      policy.projects.allowInsert.always();
      if (timing === "ordinary-scope-upsert") policy.projects.allowUpdate.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.notes.allowUpdate.always();
      policy.notes.allowDelete.always();
      policy.linkedNotes.allowRead.always();
      policy.linkedNotes.allowInsert.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...policies },
      });
      const account = await localAccountConfig(server.appId, server.url);
      const crypto = await createBrowserCrypto();
      let encrypted = 0;
      let decrypted = 0;
      let failEncryption = false;
      let failDecryption = false;
      let duringToken: (() => Promise<void>) | undefined;
      let saved: string | null = null;
      db = await createDb({
        ...account,
        e2ee: {
          app,
          store: {
            async read() {
              return saved;
            },
            async update(transform) {
              saved = transform(saved);
            },
          },
          crypto: {
            ...crypto,
            equalityIndex: {
              mechanism: crypto.equalityIndex!.mechanism,
              async token(...args) {
                const token = await crypto.equalityIndex!.token(...args);
                const action = duringToken;
                duringToken = undefined;
                await action?.();
                return token;
              },
            },
            ...(timing === "equality-collision" || timing === "equality-subscription-collision"
              ? {
                  equalityIndex: {
                    mechanism: crypto.equalityIndex!.mechanism,
                    async token() {
                      // Deliberately make every candidate collide through the public BYOC seam.
                      return encodeEnvelope(crypto.equalityIndex!.mechanism, new Uint8Array(32));
                    },
                  },
                }
              : {}),
            cellCipher: {
              mechanism: crypto.cellCipher.mechanism,
              async encrypt(...args) {
                encrypted++;
                const ciphertext = await crypto.cellCipher.encrypt(...args);
                if (timing === "ordinary-insert-error" || failEncryption)
                  throw new Error("Test cell encryption failure");
                // Enrolment has completed, but the initialisation transaction
                // cannot reach the authority after its ciphertext is prepared.
                if (timing === "offline-at-submit" && encrypted === 2) await server.stop();
                return ciphertext;
              },
              async decrypt(...args) {
                decrypted++;
                if (failDecryption) throw new Error("Test cell decryption failure");
                const plaintext = await crypto.cellCipher.decrypt(...args);
                if (timing !== "byoc-buffer") return plaintext;
                const buffer = Buffer.from(plaintext);
                plaintext.fill(0);
                return buffer;
              },
            },
          },
        },
      });
      if (timing.startsWith("ordinary-insert") || timing === "ordinary-update") {
        const project = db.insert(app.projects, { title: "Project" });
        expect(project).not.toBeInstanceOf(Promise);
        await project.wait({ tier: "global" });
        expect(
          await db.e2ee.explain({ scope: app.projects, identifier: project.value.id }),
        ).toEqual({ state: "ready" });
        const note = db.insert(app.notes, {
          projectId: project.value.id,
          title: "Ordinary encrypted insert",
          done: false,
          payload: new Uint8Array([7, 8, 9]),
        });
        expect(note).not.toBeInstanceOf(Promise);
        expect(note.value.title).toBe("Ordinary encrypted insert");
        if (timing === "ordinary-insert-error") {
          await expect(note.wait({ tier: "global" })).rejects.toThrow(
            "Could not encrypt the value",
          );
          expect(await db.all(app.notes, { tier: "global" })).toEqual([]);
          expect(encrypted).toBeGreaterThan(0);
          return;
        }
        await note.wait({ tier: "global" });
        expect(await db.one(app.notes.where({ id: note.value.id }), { tier: "global" })).toEqual(
          note.value,
        );
        expect(encrypted).toBeGreaterThan(0);
        expect(decrypted).toBeGreaterThan(0);
        if (timing === "ordinary-update") {
          const beforePlaintextUpdate = encrypted;
          const plaintext = db.update(app.notes, note.value.id, { done: true });
          expect(plaintext).not.toBeInstanceOf(Promise);
          await plaintext.wait({ tier: "global" });
          expect(encrypted).toBe(beforePlaintextUpdate);
          const update = db.update(app.notes, note.value.id, {
            title: "Changed encrypted title",
            payload: new Uint8Array([10, 11]),
          });
          expect(update).not.toBeInstanceOf(Promise);
          await update.wait({ tier: "global" });
          expect(
            await db.one(app.notes.where({ id: note.value.id }), { tier: "global" }),
          ).toMatchObject({
            title: "Changed encrypted title",
            payload: new Uint8Array([10, 11]),
            done: true,
          });
          const tx = db.beginTransaction();
          expect(
            tx.update(app.notes, note.value.id, { title: "Transaction update" }),
          ).toBeUndefined();
          await tx.commit().wait({ tier: "global" });
          expect(
            await db.one(app.notes.where({ id: note.value.id }), { tier: "global" }),
          ).toMatchObject({
            title: "Transaction update",
            payload: new Uint8Array([10, 11]),
            done: true,
          });
          const other = db.insert(app.projects, { title: "Other project" });
          await other.wait({ tier: "global" });
          await expect(
            db
              .update(app.notes, note.value.id, { projectId: other.value.id })
              .wait({ tier: "global" }),
          ).rejects.toThrow("immutable");
          const beforeSameSpace = encrypted;
          await db
            .update(app.notes, note.value.id, { projectId: project.value.id, done: false })
            .wait({ tier: "global" });
          expect(encrypted).toBe(beforeSameSpace);
          failEncryption = true;
          const failed = db.update(app.notes, note.value.id, {
            title: "Must not replace the value",
          });
          expect(failed).not.toBeInstanceOf(Promise);
          await expect(failed.wait({ tier: "global" })).rejects.toThrow(
            "Could not encrypt the value",
          );
          failEncryption = false;
          expect(
            await db.one(app.notes.where({ id: note.value.id }), { tier: "global" }),
          ).toMatchObject({
            projectId: project.value.id,
            title: "Transaction update",
            done: false,
          });
        }
        return;
      }
      const scopeId = globalThis.crypto.randomUUID();
      if (timing === "ordinary-scope-upsert") {
        const data = { title: "Project" };
        const first = db.upsert(app.projects, scopeId, data);
        data.title = "Changed after upsert returned";
        expect(first).not.toBeInstanceOf(Promise);
        await first.wait({ tier: "global" });
        expect(await db.one(app.projects.where({ id: scopeId }), { tier: "global" })).toEqual({
          id: scopeId,
          title: "Project",
        });
        expect(await db.e2ee.explain({ scope: app.projects, identifier: scopeId })).toEqual({
          state: "ready",
        });
        await db.upsert(app.projects, scopeId, { title: "Project" }).wait({ tier: "global" });
        expect(await db.all(app.__e2ee_spaces, { tier: "global" })).toHaveLength(1);
        expect(await db.all(app.__e2ee_space_grants, { tier: "global" })).toHaveLength(1);
      }
      const tx = db.beginExclusiveTransaction();
      const event =
        timing === "plaintext-first"
          ? tx.insert(app.events, { message: "Created together" })
          : undefined;
      if (timing === "after-yield") await Promise.resolve();
      if (timing === "transaction-scope-upsert") {
        const data = { title: "Project" };
        tx.upsert(app.projects, scopeId, data);
        data.title = "Changed after upsert returned";
      }
      const project = timing.endsWith("scope-upsert")
        ? { id: scopeId, title: "Project" }
        : tx.insert(app.projects, { title: "Project" });
      const note = tx.insert(app.notes, {
        projectId: project.id,
        title: "Private title",
        done: false,
        payload: new Uint8Array([1, 2, 255]),
      });
      expect(project).not.toBeInstanceOf(Promise);
      expect(note).not.toBeInstanceOf(Promise);
      if (timing === "equality-transaction-initial") {
        expect(
          await tx.all(
            app.notes
              .where({ projectId: project.id, title: "Private title" })
              .select("id", "title"),
            { tier: "local" },
          ),
        ).toEqual([{ id: note.id, title: note.title }]);
      }
      if (timing === "transaction-read") {
        expect(await tx.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual(note);
      }
      if (timing === "transaction-projection") {
        expect(
          await tx.one(app.notes.where({ id: note.id }).select("id", "title"), { tier: "local" }),
        ).toEqual({ id: note.id, title: "Private title" });
      }
      const committed = tx.commit();
      expect(committed).not.toBeInstanceOf(Promise);
      if (timing === "offline-at-submit") {
        const localWait = committed.wait({ tier: "local" });
        localWait.catch(() => {});
        await expect
          .poll(async () => (await db!.all(app.projects, { tier: "local" })).length, {
            timeout: 10_000,
          })
          .toBe(1);
        expect(encrypted).toBeGreaterThan(0);
        // Physical local visibility is not authoritative acceptance. Give an
        // incorrectly local-only wait time to settle before checking the floor.
        expect(
          await Promise.race([
            localWait.then(
              () => "resolved",
              () => "rejected",
            ),
            new Promise<string>((resolve) => setTimeout(() => resolve("pending"), 100)),
          ]),
        ).toBe("pending");
        return;
      }
      await committed.wait({ tier: "global" });
      if (timing === "equality-transaction-initial" || timing === "equality-transaction-existing") {
        const reader = db.beginTransaction();
        try {
          expect(
            await reader.all(app.notes.where({ projectId: project.id, title: "Private title" }), {
              tier: "local",
            }),
          ).toEqual([note]);
        } finally {
          await reader.rollback();
        }
        return;
      }
      if (timing === "equality-epoch-race" || timing === "equality-subscription-epoch") {
        const recipientAccount = await localAccountConfig(server.appId, server.url);
        let recipientSaved: string | null = null;
        const recipient = await createDb({
          ...recipientAccount,
          e2ee: {
            app,
            store: {
              async read() {
                return recipientSaved;
              },
              async update(transform) {
                recipientSaved = transform(recipientSaved);
              },
            },
          },
        });
        try {
          await recipient.e2ee.devices.list();
          await db.e2ee.spaces.grant(app.projects, project.id, recipientAccount.account.id).wait();
          if (timing === "equality-subscription-epoch") {
            const snapshots: unknown[][] = [];
            let failure: Error | undefined;
            const stop = db.subscribe(
              app.notes.where({ projectId: project.id, title: "After rotation" }),
              {
                onUpdate: (rows) => snapshots.push(rows),
                onError: (error) => {
                  failure = error;
                },
              },
              { tier: "global" },
            );
            try {
              await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 }).toEqual([]);
              await db.e2ee.spaces
                .revoke(app.projects, project.id, recipientAccount.account.id)
                .wait();
              expect(
                await db.e2ee.explain({ scope: app.projects, identifier: project.id }),
              ).toEqual({ state: "ready" });
              const added = db.insert(app.notes, {
                projectId: project.id,
                title: "After rotation",
                done: false,
                payload: new Uint8Array([9]),
              });
              await added.wait({ tier: "global" });
              await expect
                .poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 })
                .toEqual([added.value]);
              expect(failure).toBeUndefined();
            } finally {
              stop();
            }
            return;
          }
          duringToken = async () => {
            await db!.e2ee.spaces
              .revoke(app.projects, project.id, recipientAccount.account.id)
              .wait();
            expect(await db!.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual(
              { state: "ready" },
            );
            await db!
              .insert(app.notes, {
                projectId: project.id,
                title: "After rotation",
                done: false,
                payload: new Uint8Array([9]),
              })
              .wait({ tier: "global" });
          };
          const rows = await db.all(
            app.notes.where({ projectId: project.id, title: "After rotation" }),
            { tier: "global" },
          );
          expect(rows).toHaveLength(1);
          expect(rows[0]).toMatchObject({ projectId: project.id, title: "After rotation" });
        } finally {
          await recipient.shutdown();
        }
        return;
      }
      if (timing.startsWith("equality-device-invalidation")) {
        const subscriberAccount = timing.endsWith("recipient")
          ? await localAccountConfig(server.appId, server.url)
          : account;
        let subscriberSaved: string | null = null;
        const subscriber =
          subscriberAccount === account
            ? db
            : await createDb({
                ...subscriberAccount,
                e2ee: {
                  app,
                  store: {
                    async read() {
                      return subscriberSaved;
                    },
                    async update(transform) {
                      subscriberSaved = transform(subscriberSaved);
                    },
                  },
                },
              });
        const [original] = await subscriber.e2ee.devices.list();
        if (subscriber !== db)
          await db.e2ee.spaces.grant(app.projects, project.id, subscriberAccount.account.id).wait();
        let saved: string | null = null;
        const other = await createDb({
          ...subscriberAccount,
          e2ee: {
            app,
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
        try {
          const pending = (await other.e2ee.devices.list()).find(
            (device) => device.state === "pending",
          )!;
          await subscriber.e2ee.devices.approve(pending.id).wait();
          const snapshots: unknown[][] = [];
          let failure: Error | undefined;
          const stop = subscriber.subscribe(
            app.notes.where({ projectId: project.id, title: note.title }),
            {
              onUpdate: (rows) => snapshots.push(rows),
              onError: (error) => {
                failure = error;
              },
            },
            { tier: "global" },
          );
          try {
            await expect
              .poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 })
              .toEqual([note]);
            await other.e2ee.devices.revoke(original!.id).wait();
            await expect.poll(() => failure?.name, { timeout: 10_000 }).toBe("E2eeDataError");
            expect(snapshots.some((rows) => rows.length === 0)).toBe(false);
          } finally {
            stop();
          }
        } finally {
          await other.shutdown();
          if (subscriber !== db) await subscriber.shutdown();
        }
        return;
      }
      if (timing === "equality-membership-invalidation") {
        const snapshots: unknown[][] = [];
        let failure: Error | undefined;
        const stop = db.subscribe(
          app.notes.where({ projectId: project.id, title: note.title }),
          {
            onUpdate: (rows) => snapshots.push(rows),
            onError: (error) => {
              failure = error;
            },
          },
          { tier: "global" },
        );
        try {
          await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 }).toEqual([note]);
          await db.e2ee.spaces.revoke(app.projects, project.id, account.account.id).wait();
          await expect.poll(() => failure?.name, { timeout: 10_000 }).toBe("E2eeDataError");
          expect(snapshots.some((rows) => rows.length === 0)).toBe(false);
        } finally {
          stop();
        }
        return;
      }
      if (timing === "equality-owned-bytes") {
        const snapshots: Array<Array<{ id: string; payload: Uint8Array }>> = [];
        let failure: Error | undefined;
        const stop = db.subscribe(
          app.notes.where({ projectId: project.id, payload: { eq: new Uint8Array([1, 2, 255]) } }),
          {
            onUpdate: (rows) => snapshots.push(rows),
            onError: (error) => {
              failure = error;
            },
          },
          { tier: "global" },
        );
        try {
          await expect
            .poll(() => failure ?? snapshots.at(-1)?.map((row) => row.id), { timeout: 10_000 })
            .toEqual([note.id]);
          snapshots.at(-1)![0]!.payload.fill(0);
          const next = db.insert(app.notes, {
            projectId: project.id,
            title: "Another match",
            done: false,
            payload: new Uint8Array([1, 2, 255]),
          });
          await next.wait({ tier: "global" });
          await expect
            .poll(
              () =>
                failure ??
                snapshots
                  .at(-1)
                  ?.map((row) => row.id)
                  .sort(),
              { timeout: 10_000 },
            )
            .toEqual([note.id, next.value.id].sort());
        } finally {
          stop();
        }
        return;
      }
      if (timing === "equality-json") {
        const documents = [
          { label: "ordered", body: { x: 1, y: 2 } },
          { label: "reordered", body: { y: 2, x: 1 } },
          { label: "spaced", body: '{ "x": 1, "y": 2 }' },
        ];
        for (const document of documents) {
          await db
            .insert(app.publicDocuments, { projectId: project.id, ...document })
            .wait({ tier: "global" });
          await db
            .insert(app.privateDocuments, { projectId: project.id, ...document })
            .wait({ tier: "global" });
        }
        for (const { body, label } of documents) {
          const ordinary = await db.all(
            app.publicDocuments.where({ projectId: project.id, body: { eq: body } }),
            { tier: "global" },
          );
          expect(ordinary.map((row) => row.label)).toEqual([label]);
          const encrypted = await db.all(
            app.privateDocuments.where({ projectId: project.id, body: { eq: body } }),
            { tier: "global" },
          );
          expect(encrypted.map((row) => row.label)).toEqual([label]);
        }
        const query = app.privateDocuments.where({
          projectId: project.id,
          body: { eq: '{ "x": 1, "y": 2 }' },
        });
        const reader = db.beginTransaction();
        try {
          expect((await reader.all(query, { tier: "local" })).map((row) => row.label)).toEqual([
            "spaced",
          ]);
        } finally {
          await reader.rollback();
        }
        const snapshots: string[][] = [];
        let failure: Error | undefined;
        const stop = db.subscribe(
          query,
          {
            onUpdate: (rows) => snapshots.push(rows.map((row) => row.label).sort()),
            onError: (error) => {
              failure = error;
            },
          },
          { tier: "global" },
        );
        try {
          await expect
            .poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 })
            .toEqual(["spaced"]);
          await db
            .insert(app.privateDocuments, {
              projectId: project.id,
              label: "second",
              body: '{ "x": 1, "y": 2 }',
            })
            .wait({ tier: "global" });
          await expect
            .poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 })
            .toEqual(["second", "spaced"]);
        } finally {
          stop();
        }
        return;
      }
      if (timing === "equality-nonfinite") {
        const reader = db;
        await db
          .insert(app.optionalMeasurements, { projectId: project.id, value: null })
          .wait({ tier: "global" });
        for (const value of [NaN, Infinity, -Infinity]) {
          await expect(async () =>
            reader.all(app.optionalMeasurements.where({ projectId: project.id, value }), {
              tier: "global",
            }),
          ).rejects.toThrow("Non-finite numbers are not supported in encrypted query predicates");
        }
        return;
      }
      if (timing === "equality-omitted-optional") {
        const omitted = db.insert(app.optionalNotes, { projectId: project.id });
        const explicit = db.insert(app.optionalNotes, { projectId: project.id, title: null });
        const empty = db.insert(app.optionalNotes, { projectId: project.id, title: "" });
        await Promise.all([omitted, explicit, empty].map((row) => row.wait({ tier: "global" })));
        const rows = await db.all(
          app.optionalNotes.where({ projectId: project.id, title: { eq: null } }),
          { tier: "global" },
        );
        expect(rows.map((row) => row.id).sort()).toEqual(
          [omitted.value.id, explicit.value.id].sort(),
        );
        expect(rows.every((row) => row.title === null)).toBe(true);
        return;
      }
      if (timing === "equality-null") {
        const absent = db.insert(app.optionalNotes, { projectId: project.id, title: null });
        const empty = db.insert(app.optionalNotes, { projectId: project.id, title: "" });
        const text = db.insert(app.optionalNotes, { projectId: project.id, title: "Present" });
        await Promise.all([absent, empty, text].map((row) => row.wait({ tier: "global" })));
        for (const row of [absent, empty, text]) {
          expect(
            await db.all(
              app.optionalNotes.where({ projectId: project.id, title: { eq: row.value.title } }),
              { tier: "global" },
            ),
          ).toEqual([row.value]);
        }
        return;
      }
      if (timing === "equality-composite-literals") {
        const instant = new Date("2026-01-02T03:04:05.006Z");
        const row = db.insert(app.observations, {
          projectId: project.id,
          numbers: [9007199254740993n],
          tagged: { type: "count", value: 9007199254740993n },
          time: instant,
        });
        await row.wait({ tier: "global" });
        expect(
          await db.all(
            app.observations.where({ projectId: project.id, numbers: [9007199254740993n] }),
            { tier: "global" },
          ),
        ).toEqual([row.value]);
        expect(
          await db.all(
            app.observations.where({
              projectId: project.id,
              tagged: { match: { type: "count", where: { value: 9007199254740993n } } },
            }),
            { tier: "global" },
          ),
        ).toEqual([row.value]);
        expect(
          await db.all(app.observations.where({ projectId: project.id, time: instant }), {
            tier: "global",
          }),
        ).toEqual([row.value]);
        expect(
          await db.all(
            app.observations.where({
              projectId: project.id,
              time: new Date(instant.getTime() + 1),
            }),
            { tier: "global" },
          ),
        ).toEqual([]);
        return;
      }
      if (timing === "equality-bigint") {
        const counter = db.insert(app.counters, {
          projectId: project.id,
          value: 9007199254740993n,
        });
        await counter.wait({ tier: "global" });
        expect(
          await db.all(app.counters.where({ projectId: project.id, value: 9007199254740993n }), {
            tier: "global",
          }),
        ).toEqual([counter.value]);
        expect(
          await db.all(app.counters.where({ projectId: project.id, value: 9007199254740992n }), {
            tier: "global",
          }),
        ).toEqual([]);
        return;
      }
      if (timing === "equality-enum-zero") {
        const reading = db.insert(app.readings, {
          projectId: project.id,
          reading: { type: "measured", value: -0 },
        });
        await reading.wait({ tier: "global" });
        expect(
          await db.all(
            app.readings.where({
              projectId: project.id,
              reading: { match: { type: "measured", where: { value: 0 } } },
            }),
            { tier: "global" },
          ),
        ).toEqual([reading.value]);
        expect(
          await db.all(
            app.readings.where({
              projectId: project.id,
              reading: { match: { type: "measured", where: { value: 1 } } },
            }),
            { tier: "global" },
          ),
        ).toEqual([]);
        await expect(
          db.all(
            app.readings.where({
              projectId: project.id,
              reading: { match: { type: "measured" } },
            }),
            { tier: "global" },
          ),
        ).rejects.toThrow("Unsupported encrypted query");
        return;
      }
      if (timing === "equality-array-zero") {
        const sample = db.insert(app.samples, { projectId: project.id, values: [-0, 2] });
        await sample.wait({ tier: "global" });
        for (const values of [
          [0, 2],
          [-0, 2],
        ]) {
          expect(
            await db.all(app.samples.where({ projectId: project.id, values }), { tier: "global" }),
          ).toEqual([sample.value]);
        }
        expect(
          await db.all(app.samples.where({ projectId: project.id, values: [0, 3] }), {
            tier: "global",
          }),
        ).toEqual([]);
        return;
      }
      if (timing === "equality-negative-zero") {
        const measurement = db.insert(app.measurements, { projectId: project.id, value: -0 });
        await measurement.wait({ tier: "global" });
        for (const value of [0, -0]) {
          expect(
            await db.all(app.measurements.where({ projectId: project.id, value }), {
              tier: "global",
            }),
          ).toEqual([measurement.value]);
        }
        return;
      }
      if (timing.startsWith("equality-") && !timing.startsWith("equality-subscription")) {
        const other = db.insert(app.notes, {
          projectId: project.id,
          title: "Other title",
          done: false,
          payload: new Uint8Array([3]),
        });
        await other.wait({ tier: "global" });
        const query = app.notes.where({ projectId: project.id, title: "Private title" });
        expect(await db.all(query, { tier: "global" })).toEqual([note]);
        await db.update(app.notes, note.id, { done: true }).wait({ tier: "global" });
        // The false-positive candidate sorts first, but cannot consume the logical limit.
        expect(
          await db.all(query.orderBy("done").limit(1).select("id", "title"), { tier: "global" }),
        ).toEqual([{ id: note.id, title: note.title }]);
        expect(await db.all(query.offset(1), { tier: "global" })).toEqual([]);
        expect(
          await db.all(app.notes.where({ projectId: project.id, title: "Absent" }), {
            tier: "global",
          }),
        ).toEqual([]);
        await expect(
          db.all(app.notes.where({ title: "Private title" }), { tier: "global" }),
        ).rejects.toThrow("space");
        await db.update(app.notes, note.id, { title: "Changed title" }).wait({ tier: "global" });
        expect(await db.all(query, { tier: "global" })).toEqual([]);
        const changed = app.notes.where({ projectId: project.id, title: "Changed title" });
        expect(await db.all(changed, { tier: "global" })).toEqual([
          { ...note, title: "Changed title", done: true },
        ]);
        failEncryption = true;
        await expect(
          db.update(app.notes, note.id, { title: "Failed title" }).wait({ tier: "global" }),
        ).rejects.toThrow("Could not encrypt the value");
        failEncryption = false;
        expect(await db.all(changed, { tier: "global" })).toEqual([
          { ...note, title: "Changed title", done: true },
        ]);
        return;
      }
      if (timing === "nested-reference") {
        const linked = db.insert(app.linkedNotes, { project: project.id, title: "Linked secret" });
        await linked.wait({ tier: "global" });
        const query = app.projects.where({ id: project.id }).include({
          linkedNotesViaProject: app.linkedNotes.include({ projectRelation: true }),
        });
        const expected = {
          ...project,
          linkedNotesViaProject: [{ ...linked.value, projectRelation: project }],
        };
        expect(await db.one(query, { tier: "global" })).toEqual(expected);
        const reader = db.beginTransaction();
        try {
          expect(await reader.one(query, { tier: "local" })).toEqual(expected);
        } finally {
          await reader.rollback();
        }
        return;
      }
      if (timing.startsWith("nested-")) {
        const projected = timing === "nested-projection";
        const query = app.projects.where({ id: project.id }).include({
          notesViaProject: projected ? app.notes.select("id", "title") : true,
        });
        const child = projected ? { id: note.id, title: note.title } : note;
        const expected = { ...project, notesViaProject: [child] };
        expect(await db.one(query, { tier: "global" })).toEqual(expected);
        const reader = db.beginTransaction();
        try {
          expect(await reader.one(query, { tier: "local" })).toEqual(expected);
        } finally {
          await reader.rollback();
        }
        const snapshots: unknown[][] = [];
        let failure: Error | undefined;
        const stop = db.subscribe(
          query,
          {
            onUpdate: (rows) => snapshots.push(rows),
            onError: (error) => {
              failure = error;
            },
          },
          { tier: "global" },
        );
        try {
          await expect
            .poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 })
            .toEqual([expected]);
          await db.update(app.notes, note.id, { title: "Nested update" }).wait({ tier: "global" });
          await expect
            .poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 })
            .toEqual([{ ...project, notesViaProject: [{ ...child, title: "Nested update" }] }]);
          expect(failure).toBeUndefined();
        } finally {
          stop();
        }
        return;
      }
      if (event) {
        expect(await db.one(app.events.where({ id: event.id }), { tier: "global" })).toEqual(event);
      }
      if (timing === "projection") {
        expect(
          await db.one(app.notes.where({ id: note.id }).select("id", "title"), {
            tier: "global",
          }),
        ).toEqual({ id: note.id, title: "Private title" });
        const target = { scope: app.projects, identifier: project.id };
        // Concurrent delivery checks may follow the read's background maintenance.
        expect(
          await Promise.allSettled([db.e2ee.explain(target), db.e2ee.explain(target)]),
        ).toEqual([
          { status: "fulfilled", value: { state: "ready" } },
          { status: "fulfilled", value: { state: "ready" } },
        ]);
      }
      if (timing === "transaction-scope-upsert") {
        expect(await db.one(app.projects.where({ id: scopeId }), { tier: "global" })).toEqual({
          id: scopeId,
          title: "Project",
        });
      }
      expect(await db.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
        state: "ready",
      });
      expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toMatchObject({
        id: note.id,
        projectId: project.id,
        title: "Private title",
        done: false,
        payload: new Uint8Array([1, 2, 255]),
      });
      expect(encrypted).toBeGreaterThan(0);
      expect(decrypted).toBeGreaterThan(0);
      if (timing === "ordinary-restore") {
        await db.delete(app.notes, note.id).wait({ tier: "global" });
        const data = {
          projectId: note.projectId,
          title: "Restored",
          done: note.done,
          payload: note.payload,
        };
        const restored = db.restore(app.notes, note.id, data);
        expect(restored).not.toBeInstanceOf(Promise);
        expect(restored.value).toEqual({ ...note, title: "Restored" });
        await restored.wait({ tier: "global" });
        expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual({
          ...note,
          title: "Restored",
        });
        await db.delete(app.notes, note.id).wait({ tier: "global" });
        await expect(
          db
            .restore(app.notes, note.id, {
              ...data,
              projectId: globalThis.crypto.randomUUID(),
            })
            .wait({ tier: "global" }),
        ).rejects.toThrow("The encryption space of a row is immutable");
        expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toBeNull();
      }
      if (timing === "ordinary-upsert") {
        const id = globalThis.crypto.randomUUID();
        const data = {
          projectId: project.id,
          title: "Upserted",
          done: false,
          payload: new Uint8Array([3, 4]),
        };
        const created = db.upsert(app.notes, id, data);
        expect(created).not.toBeInstanceOf(Promise);
        await created.wait({ tier: "global" });
        expect(await db.one(app.notes.where({ id }), { tier: "global" })).toEqual({ id, ...data });
        await db.upsert(app.notes, id, { title: "Changed" }).wait({ tier: "global" });
        expect(await db.one(app.notes.where({ id }), { tier: "global" })).toEqual({
          id,
          ...data,
          title: "Changed",
        });
        await expect(
          db
            .upsert(app.notes, id, { projectId: globalThis.crypto.randomUUID() })
            .wait({ tier: "global" }),
        ).rejects.toThrow("The encryption space of a row is immutable");
      }
      if (timing.startsWith("equality-subscription")) {
        if (timing === "equality-subscription-collision") {
          await db.update(app.notes, note.id, { done: true }).wait({ tier: "global" });
          await db
            .insert(app.notes, {
              projectId: project.id,
              title: "False candidate",
              done: false,
              payload: new Uint8Array([3]),
            })
            .wait({ tier: "global" });
        }
        const snapshots: unknown[][] = [];
        let failure: Error | undefined;
        const stop = db.subscribe(
          app.notes
            .where({ projectId: project.id, title: note.title })
            .orderBy("done")
            .select("id", "title")
            .limit(1),
          {
            onUpdate: (rows) => snapshots.push(rows),
            onError: (error) => {
              failure = error;
            },
          },
          { tier: "global" },
        );
        try {
          await expect
            .poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 })
            .toEqual([{ id: note.id, title: note.title }]);
          await db
            .update(app.notes, note.id, { title: "No longer matches" })
            .wait({ tier: "global" });
          await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 }).toEqual([]);
          await db.update(app.notes, note.id, { title: note.title }).wait({ tier: "global" });
          await expect
            .poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 })
            .toEqual([{ id: note.id, title: note.title }]);
          expect(failure).toBeUndefined();
        } finally {
          stop();
        }
      }
      if (timing.startsWith("subscription")) {
        const projected = timing === "subscription-projection";
        const query = app.notes.where({ id: note.id });
        const expected = projected ? { id: note.id, title: note.title } : note;
        const snapshots: unknown[][] = [];
        let failure: Error | undefined;
        failDecryption = timing === "subscription-error";
        const stop = db.subscribe(
          projected ? query.select("id", "title") : query,
          {
            onUpdate: (rows) => snapshots.push(rows),
            onError: (error) => {
              failure = error;
            },
          },
          { tier: "global" },
        );
        try {
          if (failDecryption) {
            await expect
              .poll(() => failure?.message, { timeout: 10_000 })
              .toBe("Encrypted data could not be authenticated or decoded");
            expect(failure).toMatchObject({ name: "E2eeDataError", code: "invalid-ciphertext" });
            expect(failure).not.toHaveProperty("cause");
            expect(snapshots).toEqual([]);
          } else {
            await expect
              .poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 })
              .toEqual([expected]);
            await db
              .update(app.notes, note.id, { title: "Subscription update" })
              .wait({ tier: "global" });
            await expect
              .poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 })
              .toEqual([{ ...expected, title: "Subscription update" }]);
            expect(failure).toBeUndefined();
          }
        } finally {
          stop();
        }
      }
      if (timing === "transaction-read") {
        const reader = db.beginTransaction();
        try {
          expect(await reader.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual(
            note,
          );
        } finally {
          await reader.rollback();
        }
      }
    } finally {
      await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);
