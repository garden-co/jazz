import { expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer, type Socket } from "node:net";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import {
  app,
  permissions,
  indexedApp,
  groupApp,
  groupPolicies,
} from "./fixtures/encrypted-offline-fixture-schema.js";

it.each([
  { reopenAt: "after a warming read", app, permissions },
  { reopenAt: "immediately after creation", app, permissions },
  { reopenAt: "without explicit disconnection", app, permissions },
  {
    reopenAt: "with an equality query without explicit disconnection",
    app: indexedApp,
    permissions,
  },
  { reopenAt: "while online preparation stalls", app, permissions },
  {
    reopenAt: "immediately after creation with group tables",
    app: groupApp,
    permissions: { ...permissions, ...groupPolicies },
  },
  {
    reopenAt: "immediately after creation with a group recipient",
    app: groupApp,
    permissions: { ...permissions, ...groupPolicies },
  },
])(
  "reopens a persisted native database offline $reopenAt and updates its encrypted data",
  async ({ reopenAt, app, permissions }) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const directory = await mkdtemp(join(tmpdir(), "jazz-e2ee-offline-reopen-"));
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
    const config = {
      appId: server.appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "persistent" as const, dataPath: join(directory, "database") },
      initial: "local-first" as const,
      store: store(),
      e2ee: { app, store: store() },
    };
    let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let stalledServer: ReturnType<typeof createServer> | undefined;
    const stalledSockets = new Set<Socket>();
    let connected!: () => void;
    const connectionStarted = new Promise<void>((resolve) => {
      connected = resolve;
    });
    let listingFinished = false;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      owner = await createJazzSession(config);
      const before = owner.getSnapshot().client!.db;
      const accountId = owner.getSnapshot().account!.id;
      const group =
        reopenAt === "immediately after creation with a group recipient"
          ? await before.e2ee.groups.create().wait()
          : undefined;
      const tx = before.beginExclusiveTransaction();
      const project = tx.insert(
        app.projects,
        { title: "Persisted project" },
        group ? { initialRecipients: [group.id] } : undefined,
      );
      const note = tx.insert(app.notes, { projectId: project.id, title: "Before reopening" });
      await tx.commit().wait({ tier: "global" });
      if (reopenAt === "after a warming read") {
        expect(await before.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
          state: "ready",
        });
        expect(await before.one(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual(
          note,
        );
      }
      await owner.close();
      owner = undefined;
      await server.stop();

      if (reopenAt === "while online preparation stalls") {
        // Hold the transport outside Jazz, without replacing its query/runtime paths.
        stalledServer = createServer((socket) => {
          stalledSockets.add(socket);
          socket.on("error", () => {});
          connected();
        });
        await new Promise<void>((resolve, reject) => {
          stalledServer!.once("error", reject);
          stalledServer!.listen(Number(new URL(server.url).port), "127.0.0.1", resolve);
        });
      }
      owner = await createJazzSession(config);
      const reopened = owner.getSnapshot().client!.db;
      expect(owner.getSnapshot().account!.id).toBe(accountId);
      if (
        reopenAt !== "without explicit disconnection" &&
        reopenAt !== "with an equality query without explicit disconnection" &&
        reopenAt !== "while online preparation stalls"
      )
        await reopened.disconnect();
      // The ordinary persisted row is a control for reopening without the server.
      expect(
        await reopened.one(app.projects.where({ id: project.id }), { tier: "local-first" }),
      ).toEqual(project);
      if (stalledServer) {
        reopened.e2ee.devices.list().then(
          () => {
            listingFinished = true;
          },
          () => {
            listingFinished = true;
          },
        );
        await connectionStarted;
        expect(listingFinished).toBe(false);
      }
      if (reopenAt === "with an equality query without explicit disconnection") {
        expect(
          await reopened.all(
            app.notes.where({ projectId: project.id, title: "Before reopening" }),
            { tier: "local-first" },
          ),
        ).toEqual([note]);
      }
      const write = reopened.update(app.notes, note.id, { title: "After reopening offline" });
      expect(
        await Promise.race([
          write.wait({ tier: "local" }).then(() => "stored locally"),
          new Promise<string>((resolve) => {
            timer = setTimeout(() => resolve("still waiting"), 5_000);
          }),
        ]),
      ).toBe("stored locally");
      expect(await reopened.one(app.notes.where({ id: note.id }), { tier: "local-first" })).toEqual(
        {
          ...note,
          title: "After reopening offline",
        },
      );
      if (stalledServer) expect(listingFinished).toBe(false);
    } finally {
      clearTimeout(timer);
      for (const socket of stalledSockets) socket.destroy();
      if (stalledServer)
        await new Promise<void>((resolve) => stalledServer!.close(() => resolve()));
      await owner?.close();
      await server.stop();
      await rm(directory, { recursive: true, force: true });
    }
  },
  60_000,
);
