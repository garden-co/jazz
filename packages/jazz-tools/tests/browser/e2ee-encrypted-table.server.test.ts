import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../../src/e2ee/device-requests.js";
import { spaceSchema } from "../../src/e2ee/spaces.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it.each([
  "full",
  "projected",
  "equality-full",
  "equality-projected",
  "nested-full",
  "nested-projected",
])(
  "uses ordinary encrypted mutations, reads and subscriptions in Chromium (%s)",
  async (shape) => {
    const equality = shape.startsWith("equality-");
    const projected = shape.endsWith("projected");
    const nested = shape.startsWith("nested-");
    const server = await getJazzServerInfo(`e2ee-encrypted-table-${crypto.randomUUID()}`);
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, { notesViaProject: s.reverse("notes", "project") }),
      notes: s
        .table(
          { projectId: s.uuid(), title: s.string(), payload: s.bytes() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({
          space: "projectId",
          columns: ["title", "payload"],
          ...(equality ? { indexes: { title: "equality" as const } } : {}),
        }),
    });
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.notes.allowUpdate.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let stop: (() => void) | undefined;
    try {
      await deploy({
        ...server,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...permissions },
      });
      const account = await acquireBrowserTestAccount(server);
      let saved: string | null = null;
      db = await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        account,
        driver: { type: "memory" },
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
      const tx = db.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title: "Project" });
      const note = tx.insert(app.notes, {
        projectId: project.id,
        title: "Private title",
        payload: new Uint8Array([1, 2, 255]),
      });
      expect(note).not.toBeInstanceOf(Promise);
      expect(await tx.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual(note);
      const baseQuery = equality
        ? app.notes.where({ projectId: project.id, title: note.title })
        : app.notes.where({ id: note.id });
      const selected = projected ? baseQuery.select("id", "title") : baseQuery;
      const query = nested
        ? app.projects.where({ id: project.id }).include({ notesViaProject: selected })
        : selected;
      const child = projected ? { id: note.id, title: note.title } : note;
      const expected = nested ? { ...project, notesViaProject: [child] } : child;
      expect(await tx.one(query, { tier: "local" })).toEqual(expected);
      await tx.commit().wait({ tier: "global" });
      expect(await db.one(query, { tier: "global" })).toEqual(expected);
      let rows: unknown[] | undefined;
      let error: Error | undefined;
      stop = db.subscribe(
        query,
        {
          onUpdate: (next) => {
            rows = next;
          },
          onError: (next) => {
            error = next;
          },
        },
        { tier: "global" },
      );
      await expect.poll(() => error ?? rows, { timeout: 15_000 }).toEqual([expected]);
      const write = db.update(app.notes, note.id, { title: "Updated title" });
      expect(write).not.toBeInstanceOf(Promise);
      await write.wait({ tier: "global" });
      if (equality) {
        await expect.poll(() => error ?? rows, { timeout: 15_000 }).toEqual([]);
        expect(await db.one(query, { tier: "global" })).toBeNull();
        await db.update(app.notes, note.id, { title: note.title }).wait({ tier: "global" });
        await expect.poll(() => error ?? rows, { timeout: 15_000 }).toEqual([expected]);
        expect(await db.one(query, { tier: "global" })).toEqual(expected);
        return;
      }
      const updated = nested
        ? { ...project, notesViaProject: [{ ...child, title: "Updated title" }] }
        : { ...child, title: "Updated title" };
      await expect.poll(() => error ?? rows, { timeout: 15_000 }).toEqual([updated]);
      expect(await db.one(query, { tier: "global" })).toEqual(updated);
    } finally {
      stop?.();
      await db?.shutdown();
      await stopJazzServer(server.serverUrl);
    }
  },
  60_000,
);
