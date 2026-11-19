import { randomBytes } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { acceptDeviceAccounts } from "../../src/backend/index.js";
import { hostBuilt } from "./host.js";
import { testApp } from "../../src/testing/index.js";
import { loadBackendApp } from "../../src/dev/codegen-napi.js";
import { publishDeployment, type DeploymentBody } from "../../src/dev/catalogue-api.js";
import { boundApp } from "../../src/typed-app.js";
import { openBrowserSessionCore } from "../../src/session/create-jazz-session.js";
import {
  decodeWebSocketFrameBatch,
  encodeWebSocketFrameBatch,
} from "../../src/runtime/native-runtime/websocket.js";
import { isThinDb } from "../../src/runtime/local-first-upgrade.js";
import { app, inAnHour, permissionsFile, setupTestApp, tables } from "./test-app.js";

describe.skipIf(!hostBuilt && !process.env.CI)("WebSocket hibernation on a Durable Object", () => {
  const { appOn, signInAs, cleanups } = setupTestApp("hibernate");

  it("an HTTP wake recovers Alice's admitted socket by its original ID", async () => {
    const { jazz, client, core } = await appOn((request) => acceptDeviceAccounts(request));
    const alice = await client("device");
    const row = await jazz.db
      .insert(app.todos, { title: "open", owner: alice.session!.user! })
      .wait({ tier: "global" });
    let visible: string[] = [];
    const stop = alice.db.subscribe(app.todos.where({ title: "open" }), (rows) => {
      visible = rows.map((todo) => todo.id);
    });
    try {
      await vi.waitFor(() => expect(visible).toEqual([row.id]));
      const before = await inspect(core.url);
      const readerIds = before.admitted.filter((id) => {
        const bytes = Buffer.from(before.attachments[id]!);
        return bytes.length > 0 && attachmentParts(bytes).admission.guest;
      });
      expect(readerIds).toHaveLength(1);
      const readerId = readerIds[0]!;
      const snapshot = Buffer.from(before.attachments[readerId]!);
      const { admission, reads } = attachmentParts(snapshot);
      expect(admission.guest).toBe(true);
      expect(admission.trust).toBe("session");
      expect(reads).toHaveLength(1);
      expect(reads[0]!.includes(Buffer.from("open"))).toBe(true);

      const stopOther = alice.db.subscribe(app.todos.where({ title: "closed" }), () => {});
      try {
        await vi.waitFor(async () => {
          const bytes = Buffer.from((await inspect(core.url)).attachments[readerId]!);
          const { reads: both } = attachmentParts(bytes);
          expect(both).toHaveLength(2);
          expect(both).toContainEqual(reads[0]);
        });
      } finally {
        stopOther();
      }
      await vi.waitFor(async () => {
        expect(Buffer.from((await inspect(core.url)).attachments[readerId]!)).toEqual(snapshot);
      });
      await new Promise((resolve) => setTimeout(resolve, 20_000));
      const after = await inspect(core.url);
      expect(after.generation).not.toBe(before.generation);
      expect(after.admitted).toContain(readerId);
      expect(Buffer.from(after.attachments[readerId]!)).toEqual(snapshot);
    } finally {
      stop();
    }
  });

  it("delivers a removal on Alice's open-todos subscription across idle sleep", async () => {
    const { jazz, client, core, server } = await appOn((request) => acceptDeviceAccounts(request));
    const alice = await client("device");
    const todo = await jazz.db
      .insert(app.todos, { title: "open", owner: alice.session!.user! })
      .wait({ tier: "global" });
    let rows: string[] | undefined;
    const stop = alice.db.subscribe(app.todos.where({ title: "open" }), (result) => {
      rows = result.map((row) => row.id);
    });
    let byId: string[] | undefined;
    const stopById = alice.db.subscribe(app.todos.where({ id: todo.id }), (result) => {
      byId = result.map((row) => row.title);
    });
    let upgrades = 0;
    server.on("upgrade", () => {
      upgrades++;
    });
    const update = (title: string) =>
      jazz.db.update(app.todos, todo.id, { title }).wait({ tier: "global" });
    try {
      const waitForRows = (expected: string[]) =>
        vi.waitFor(() => expect(rows).toEqual(expected), { timeout: 10_000 });
      await waitForRows([todo.id]);
      await update("completed");
      await waitForRows([]);
      await update("open");
      await waitForRows([todo.id]);

      const generation = async () => {
        const reply = await fetch(core.url + "/test/generation");
        expect(reply.status).toBe(200);
        return reply.text();
      };
      const before = await generation();
      await new Promise((resolve) => setTimeout(resolve, 20_000));
      await update("completed");
      const after = await generation();
      await waitForRows([]);
      await vi.waitFor(() => expect(byId).toEqual(["completed"]));
      expect(upgrades).toBe(0);
      expect(after, "DO constructor must run again with the subscription open").not.toBe(before);
    } finally {
      stopById();
      stop();
    }
  });

  it.each(["write", "one-shot read"] as const)(
    "keeps the original subscription across sleep after a completed %s",
    async (operation) => {
      const { jazz, client, core } = await appOn(acceptDeviceAccounts);
      const alice = await client("device");
      const owner = alice.session!.user!;
      const seed = await jazz.db
        .insert(app.todos, { title: "seed", owner })
        .wait({ tier: "global" });
      let visible: string[] = [];
      const stop = alice.db.subscribe(app.todos.where({ title: "watched" }), (rows) => {
        visible = rows.map((row) => row.id);
      });
      try {
        const before = await registeredReader(core.url, true);
        if (operation === "write") {
          await alice.db.insert(app.todos, { title: "mine", owner }).wait({ tier: "global" });
        } else {
          const rows = await alice.db.all(app.todos.where({ id: seed.id }), { tier: "remote" });
          expect(rows.map((row) => row.title)).toEqual(["seed"]);
        }
        await waitForIdleEviction();
        const after = await inspect(core.url);
        expect(after.generation).not.toBe(before.generation);
        expect(after.admitted).toContain(before.id);
        const saved = attachmentParts(Buffer.from(after.attachments[before.id]!));
        expect(saved.reads).toEqual(before.reads);
        const update = await jazz.db
          .insert(app.todos, { title: "watched", owner })
          .wait({ tier: "global" });
        await vi.waitFor(() => expect(visible).toEqual([update.id]), { timeout: 10_000 });
      } finally {
        stop();
      }
    },
  );

  it("revoking Alice's session wakes the object and disconnects her original socket", async () => {
    const { jazz, client, core, origin } = await appOn((request) => {
      const userId = request.headers.get("x-test-user");
      return userId ? { userId, expiresAt: inAnHour() } : null;
    });
    signInAs(origin, "alice");
    const alice = await client();
    const stop = alice.db.subscribe(app.todos, () => {});
    try {
      const { id, generation } = await registeredReader(core.url, false);
      await waitForIdleEviction();
      const path = `/apps/${jazz.appId}/admin/accounts/revoke-sessions`;
      const reply = await fetch(core.url + path, {
        method: "POST",
        headers: { "X-Jazz-Admin-Secret": core.secret, "content-type": "application/json" },
        body: JSON.stringify({ identity: alice.session!.identity, revokedBefore: Date.now() }),
      });
      expect(reply.status).toBe(200);
      await vi.waitFor(async () => {
        const after = await inspect(core.url);
        expect(after.generation).not.toBe(generation);
        expect(after.firstEvent).toBe(path);
        expect(after.ids).not.toContain(id);
      });
    } finally {
      stop();
    }
  });

  it.each(["policy", "schema"] as const)(
    "a %s deployment wakes the object and ends the reader pinned to the old bundle",
    async (change) => {
      const { jazz, core, client } = await appOn(acceptDeviceAccounts);
      const reader = await client("device");
      const row = await jazz.db
        .insert(app.todos, { title: "visible", owner: reader.session!.user! })
        .wait({ tier: "global" });
      let visible: string[] = [];
      const stop = reader.db.subscribe(app.todos, (rows) => {
        visible = rows.map((r) => r.id);
      });
      try {
        await vi.waitFor(() => expect(visible).toEqual([row.id]));
        const before = await registeredReader(core.url, true);
        const from = boundApp(app.wasmSchema)!.schema.current.slice(0, 12);
        const next =
          change === "schema"
            ? testApp({
                tables: { ...tables, labels: { columns: { name: "string" } } },
                history: { [from]: { tables } },
                migrations: [{ from, createTables: ["labels"] }],
              })
            : app;
        const policies =
          change === "schema"
            ? permissionsFile
            : {
                format: 1,
                tables: {
                  memberships: { read: "true" },
                  todos: {
                    read: "exists memberships(account = session.user)",
                    insert: "owner = session.user",
                  },
                },
              };
        const { deployment } = await loadBackendApp(
          boundApp(next.wasmSchema)!,
          JSON.stringify(policies),
        );
        await waitForIdleEviction();
        const published = await publishDeployment(
          core.url,
          { appId: jazz.appId, adminSecret: core.secret },
          deployment as DeploymentBody,
        );
        expect(published.changed).toBe(true);
        await vi.waitFor(async () => {
          const after = await inspect(core.url);
          expect(after.generation).not.toBe(before.generation);
          expect(after.firstEvent).toBe(`/apps/${jazz.appId}/admin/bundles`);
          expect(after.ids).not.toContain(before.id);
        });
      } finally {
        stop();
      }
    },
  );

  it("revoking a guest assignment on wake closes only that guest's socket", async () => {
    const { jazz, core, client } = await appOn(acceptDeviceAccounts);
    const guest = await client("device");
    const stop = guest.db.subscribe(app.todos, () => {});
    const before = await registeredReader(core.url, true);
    const other = await client("device");
    const stopOther = other.db.subscribe(app.todos, () => {});
    try {
      const { id: otherId } = await registeredReader(core.url, true, 1, before.id);
      await waitForIdleEviction();
      const path = `/apps/${jazz.appId}/admin/accounts/revoke`;
      const identity = guest.session!.identity;
      const reply = await fetch(core.url + path, {
        method: "POST",
        headers: { "X-Jazz-Admin-Secret": core.secret, "content-type": "application/json" },
        body: JSON.stringify({ approver: identity, target: identity }),
      });
      expect(reply.status, await reply.text()).toBe(204);
      await vi.waitFor(async () => {
        const state = await inspect(core.url);
        expect(state.firstEvent).toBe(path);
        expect(state.generation).not.toBe(before.generation);
        expect(state.ids).not.toContain(before.id);
        expect(state.admitted).toContain(otherId);
      });
    } finally {
      stopOther();
      stop();
    }
  });

  it("recovers a durable write after shutdown loses its reply and subscription update", async () => {
    const { jazz, core, client, origin } = await appOn((request) => {
      const userId = request.headers.get("x-test-user");
      return userId ? { userId, expiresAt: inAnHour() } : null;
    });
    signInAs(origin, "alice");
    const writer = await client();
    const stopWriter = writer.db.subscribe(app.todos.where({ title: "writer" }), () => {});
    const before = await registeredReader(core.url, false);
    const reader = await client();
    const views: string[][] = [];
    const stopReader = reader.db.subscribe(app.todos, (rows) => views.push(rows.map((r) => r.id)));
    try {
      const { id: readerId } = await registeredReader(core.url, false, 1, before.id);
      const ids = [before.id, readerId];
      const armed = await fetch(core.url + "/test/drop-output", {
        method: "POST",
        body: JSON.stringify(ids),
      });
      expect(armed.status).toBe(204);
      const write = writer.db.insert(app.todos, { title: "once", owner: writer.session!.user! });
      const id = write.value.id;
      const confirmation = write.wait({ tier: "global" });
      let confirmed = false;
      void confirmation.then(
        () => {
          confirmed = true;
        },
        () => {},
      );
      await vi.waitFor(
        async () => {
          const dropped = (await inspect(core.url)).dropped;
          for (const socket of ids) expect(dropped?.[socket]).toBeGreaterThan(0);
          expect((await jazz.db.all(app.todos)).map((r) => r.id)).toEqual([id]);
        },
        { timeout: 10_000 },
      );
      expect(confirmed).toBe(false);
      expect(views.flat()).not.toContain(id);
      const shutdown = await fetch(core.url + "/test/shutdown", { method: "POST" });
      expect(shutdown.ok).toBe(false);
      expect((await confirmation).id).toBe(id);
      await vi.waitFor(() => expect(views.at(-1)).toEqual([id]), { timeout: 10_000 });
      const after = await inspect(core.url);
      expect(after.generation).not.toBe(before.generation);
      for (const socket of ids) expect(after.ids).not.toContain(socket);
      expect(views.filter((rows) => rows.includes(id))).toEqual([[id]]);
      expect((await jazz.db.all(app.todos)).map((r) => r.id)).toEqual([id]);
    } finally {
      stopReader();
      stopWriter();
    }
  });

  it.each([true, false])(
    "hibernates a socket with a split repair request (completed: %s)",
    async (complete) => {
      const { core, client } = await appOn(acceptDeviceAccounts);
      const reader = await client("device");
      const stop = reader.db.subscribe(app.todos, () => {});
      try {
        const { id, generation } = await registeredReader(core.url, true);
        const saved = (await inspect(core.url)).attachments[id];
        // One two-byte frame per WS batch. Split ResendWhole { rows: [] } at its tag.
        const send = async (marker: number, byte: number) => {
          const reply = await fetch(core.url + "/test/sync-frame?id=" + id, {
            method: "POST",
            body: Uint8Array.of(1, 2, marker, byte),
          });
          expect(reply.status).toBe(204);
        };
        await send(0xff, 2);
        expect((await inspect(core.url)).attachments[id]).toEqual([]);
        if (complete) {
          await send(0xfe, 0);
          expect((await inspect(core.url)).attachments[id]).toEqual(saved);
        }
        await waitForIdleEviction();
        const after = await inspect(core.url);
        expect(after.generation).not.toBe(generation);
        expect(after.admitted.includes(id)).toBe(complete);
        if (!complete) expect((await registeredReader(core.url, true)).id).not.toBe(id);
      } finally {
        stop();
      }
    },
  );

  it("keeps readers and writers resumable after large-value transfers", async () => {
    const { client, core, origin } = await appOn((request) => {
      const userId = request.headers.get("x-test-user");
      return userId ? { userId, expiresAt: inAnHour() } : null;
    });
    signInAs(origin, "alice");
    const writer = await client();
    const stopWriter = writer.db.subscribe(app.todos.where({ title: "other" }), () => {});
    const before = await registeredReader(core.url, false);
    const reader = await client();
    let title: string | undefined;
    const stopReader = reader.db.subscribe(app.todos, (rows) => {
      title = rows[0]?.title;
    });
    try {
      const other = await registeredReader(core.url, false, 1, before.id);
      const value = randomBytes(700_000).toString("base64");
      const owner = writer.session!.user!;
      expect(await writer.db.canInsert(app.todos, { title: "permission check", owner })).toBe(
        "allowed",
      );
      await writer.db.insert(app.todos, { title: value, owner }).wait({ tier: "global" });
      await vi.waitFor(() => expect(title === value).toBe(true), { timeout: 15_000 });
      await waitForIdleEviction();
      const after = await inspect(core.url);
      expect(after.generation).not.toBe(before.generation);
      expect(after.admitted).toEqual(expect.arrayContaining([before.id, other.id]));
    } finally {
      stopReader();
      stopWriter();
    }
  });

  it("finishes an upload on its original socket after eviction between chunk pushes", async () => {
    const { jazz, client, core } = await appOn(acceptDeviceAccounts);
    const alice = await client("device");
    const stop = alice.db.subscribe(app.todos, () => {});
    try {
      const before = await registeredReader(core.url, true);
      const gate = pauseAfterFirstChunkFrame(core.url, 6); // PushChunks
      const value = randomBytes(1_200_000).toString("base64");
      const write = alice.db.insert(app.todos, { title: value, owner: alice.session!.user! });
      await vi.waitFor(
        async () => {
          expect(gate.held.some((frame) => frame[0] === 6)).toBe(true);
          expect(gate.held.some((frame) => frame[0] === 4)).toBe(true); // Commit
          expect(await (await fetch(core.url + "/test/upload-receipt")).json()).toBe(true);
        },
        { timeout: 10_000 },
      );
      await waitForIdleEviction();
      const after = await inspect(core.url);
      expect(after.generation).not.toBe(before.generation);
      expect(after.admitted).toContain(before.id);
      gate.resume();
      await write.wait({ tier: "global" });
      expect((await inspect(core.url)).admitted).toContain(before.id);
      const stored = await jazz.db.all(app.todos.where({ id: write.value.id }));
      expect(stored[0]?.title === value).toBe(true);
    } finally {
      stop();
    }
  });

  it("finishes a download on its original socket after eviction between chunk requests", async () => {
    const { client, core, origin } = await appOn((request) => {
      const userId = request.headers.get("x-test-user");
      return userId ? { userId, expiresAt: inAnHour() } : null;
    });
    signInAs(origin, "alice");
    const writer = await client();
    // Larger than the 4 MiB the authority can answer in one range pull.
    const value = randomBytes(3_600_000).toString("base64");
    const row = await writer.db
      .insert(app.todos, { title: value, owner: writer.session!.user! })
      .wait({ tier: "global" });
    const alice = await client();
    const stop = alice.db.subscribe(app.todos.where({ title: "unrelated" }), () => {});
    try {
      const before = await registeredReader(core.url, false);
      const gate = pauseAfterFirstChunkFrame(core.url, 5); // ChunkRequests
      let finished = false;
      const result = alice.db
        .all(app.todos.where({ id: row.id }), { tier: "remote" })
        .then((rows) => {
          finished = true;
          return rows;
        });
      await vi.waitFor(
        () => {
          expect(gate.receivedChunks).toBe(true);
          expect(gate.held.some((frame) => frame[0] === 5)).toBe(true);
        },
        { timeout: 10_000 },
      );
      await waitForIdleEviction();
      const after = await inspect(core.url);
      expect(after.generation).not.toBe(before.generation);
      expect(after.admitted).toContain(before.id);
      expect(finished).toBe(false);
      gate.resume();
      expect((await result)[0]?.title === value).toBe(true);
      expect((await inspect(core.url)).admitted).toContain(before.id);
    } finally {
      stop();
    }
  });

  it("restores a thin subscription and keeps its backend transaction open across sleep", async () => {
    const { jazz, core, origin } = await appOn((request) => {
      const userId = request.headers.get("x-test-user");
      return userId ? { userId, expiresAt: inAnHour() } : null;
    });
    signInAs(origin, "alice");
    vi.stubGlobal("document", {});
    cleanups.push(async () => vi.unstubAllGlobals());
    let stored: string | null = null;
    const session = await openBrowserSessionCore({
      serverUrl: origin + jazz.basePath,
      store: {
        async read() {
          return stored;
        },
        async update(fn) {
          stored = fn(stored);
        },
      },
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());
    await session.loginWithBackend();
    const alice = session.getSnapshot().client!;
    expect(isThinDb(alice.db)).toBe(true);
    const owner = alice.session!.user!;
    const todo = await jazz.db.insert(app.todos, { title: "open", owner }).wait({ tier: "global" });
    const views: string[][] = [];
    const stop = alice.db.subscribe(app.todos.where({ title: "open" }), (rows) =>
      views.push(rows.map((r) => r.id)),
    );
    try {
      await vi.waitFor(() => expect(views.at(-1)).toEqual([todo.id]));
      const tx = alice.db.beginTransaction();
      const staged = tx.insert(app.todos, { title: "staged", owner });
      expect((await tx.all(app.todos.where({ title: "staged" }))).map((r) => r.id)).toEqual([
        staged.id,
      ]);
      const before = await inspect(core.url);
      const ids = before.admitted.filter((id) => {
        const bytes = before.attachments[id];
        return bytes?.[0] === 1 && bytes[1] === 1;
      });
      expect(ids).toHaveLength(1);
      const count = views.length;
      await waitForIdleEviction();
      const after = await inspect(core.url);
      expect(after.generation).not.toBe(before.generation);
      expect(after.admitted).toContain(ids[0]);
      // A completed request on this socket also drains any earlier wake output.
      await alice.db.all(app.todos.where({ id: todo.id }));
      expect(views.length).toBe(count);
      await jazz.db.update(app.todos, todo.id, { title: "closed" }).wait({ tier: "global" });
      await vi.waitFor(() => expect(views.at(-1)).toEqual([]));
      await tx.commit().wait({ tier: "global" });
      expect((await jazz.db.all(app.todos.where({ title: "staged" }))).map((r) => r.id)).toEqual([
        staged.id,
      ]);
    } finally {
      stop();
    }
  });

  it("unsubscribes on the original socket as the first message after sleep", async () => {
    const { client, core } = await appOn((request) => acceptDeviceAccounts(request));
    const alice = await client("device");
    const keep = alice.db.subscribe(app.todos.where({ title: "keep" }), () => {});
    const cancel = alice.db.subscribe(app.todos.where({ title: "cancel" }), () => {});
    try {
      const { id, generation, reads } = await registeredReader(core.url, true, 2);
      const kept = reads.find((read) => read.includes(Buffer.from("keep")));
      expect(kept).toBeDefined();
      await waitForIdleEviction();
      cancel();
      // Give the socket event a head start over the HTTP inspection. The recorded
      // first event proves which request actually woke the object.
      await new Promise((resolve) => setTimeout(resolve, 250));
      const after = await inspect(core.url);
      expect(after.generation).not.toBe(generation);
      expect(after.firstEvent).toBe(id);
      expect(after.admitted).toContain(id);
      expect(attachmentParts(Buffer.from(after.attachments[id]!)).reads).toEqual([kept]);
    } finally {
      keep();
    }
  });

  it("reconnects a live reader after its registrations exceed 16 KiB", async () => {
    const { jazz, client, core } = await appOn((request) => acceptDeviceAccounts(request));
    const alice = await client("device");
    let visible: string[] = [];
    const keep = alice.db.subscribe(app.todos.where({ title: "keep" }), (rows) => {
      visible = rows.map((row) => row.id);
    });
    try {
      const { id, generation } = await registeredReader(core.url, true);
      // Each read fits by itself. Together they exceed the per-socket limit.
      const largeFilter = "x".repeat(9 * 1024);
      const first = alice.db.subscribe(app.todos.where({ title: largeFilter + "a" }), () => {});
      try {
        const partial = await registeredReader(core.url, true, 2);
        expect(partial.id).toBe(id);
        expect(partial.reads.some((read) => read.length > 9 * 1024)).toBe(true);
        const second = alice.db.subscribe(app.todos.where({ title: largeFilter + "b" }), () => {});
        try {
          await vi.waitFor(async () => {
            const before = await inspect(core.url);
            expect(before.admitted).toContain(id);
            expect(before.attachments[id]).toEqual([]);
          });
        } finally {
          second();
        }
      } finally {
        first();
      }
      await waitForIdleEviction();
      const wake = await inspect(core.url);
      expect(wake.generation).not.toBe(generation);
      expect(wake.admitted).not.toContain(id);
      const restored = await registeredReader(core.url, true);
      expect(restored.id).not.toBe(id);
      const row = await jazz.db
        .insert(app.todos, { title: "keep", owner: alice.session!.user! })
        .wait({ tier: "global" });
      await vi.waitFor(() => expect(visible).toEqual([row.id]));
    } finally {
      keep();
    }
  });
});

async function inspect(url: string) {
  const response = await fetch(url + "/test/sockets", { method: "POST" });
  expect(response.status).toBe(200);
  return response.json() as Promise<{
    generation: string;
    firstEvent: string;
    dropped?: Record<string, number>;
    ids: string[];
    admitted: string[];
    attachments: Record<string, number[]>;
  }>;
}

async function registeredReader(url: string, guest: boolean, count = 1, except?: string) {
  return vi.waitFor(
    async () => {
      const state = await inspect(url);
      for (const id of state.admitted) {
        if (id === except) continue;
        const bytes = Buffer.from(state.attachments[id] ?? []);
        if (!bytes.length) continue;
        const { admission, reads } = attachmentParts(bytes);
        if (
          admission.guest === guest &&
          (guest || admission.revocation) &&
          reads.length === count
        ) {
          return { id, generation: state.generation, reads };
        }
      }
      throw new Error("waiting for the reader's live registration");
    },
    { timeout: 10_000 },
  );
}

/** Pause the sender between complete sync messages, even when they share a WebSocket batch. */
function pauseAfterFirstChunkFrame(coreUrl: string, tag: number) {
  const host = new URL(coreUrl).host;
  const send = WebSocket.prototype.send;
  const held: Uint8Array[] = [];
  let socket: WebSocket | undefined;
  let receivedChunks = false;
  const gate = vi
    .spyOn(WebSocket.prototype, "send")
    .mockImplementation(function (this: WebSocket, data) {
      if (
        new URL(this.url).host !== host ||
        !(data instanceof Uint8Array) ||
        (socket && this !== socket)
      ) {
        return send.call(this, data);
      }
      const frames = decodeWebSocketFrameBatch(data);
      if (!socket) {
        const first = frames.findIndex((frame) => frame[0] === tag);
        if (first < 0) return send.call(this, data);
        socket = this;
        socket.addEventListener("message", (event) => {
          if (
            event.data instanceof ArrayBuffer &&
            decodeWebSocketFrameBatch(new Uint8Array(event.data)).some((frame) => frame[0] === 7)
          ) {
            receivedChunks = true;
          }
        });
        send.call(this, encodeWebSocketFrameBatch(frames.splice(0, first + 1)));
      }
      held.push(...frames);
    });
  return {
    held,
    get receivedChunks() {
      return receivedChunks;
    },
    resume() {
      gate.mockRestore();
      for (const frame of held) socket!.send(encodeWebSocketFrameBatch([frame]));
    },
  };
}

function waitForIdleEviction() {
  return new Promise<void>((resolve) => setTimeout(resolve, 20_000));
}

/** Decode the length-prefixed sections of the v1 attachment; reads include their wire IDs. */
function attachmentParts(bytes: Buffer) {
  expect(bytes[0]).toBe(1);
  expect(bytes[1]).toBe(0); // Sync session.
  let offset = 2;
  const section = () => {
    const length = bytes.readUInt16LE(offset);
    offset += 2;
    const value = bytes.subarray(offset, offset + length);
    expect(value.length).toBe(length);
    offset += length;
    return value;
  };
  const admission = JSON.parse(section().toString());
  section(); // Join
  const reads = [];
  while (offset < bytes.length) reads.push(section());
  return { admission, reads };
}
