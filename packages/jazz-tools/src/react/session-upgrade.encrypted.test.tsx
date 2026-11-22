import React from "react";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { WebSocket as NodeWebSocket } from "undici";
import { acceptDeviceAccounts, createJazz, memory } from "../backend/index.js";
import { memoryPrivateStore } from "../encryption/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb, type LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import { app } from "../../tests/browser/apps/encrypted-tab/jazz.generated.js";
import permissions from "../../tests/browser/apps/encrypted-tab/jazz.permissions.json";
import { createJazzClient } from "../session/create-jazz-session.js";
import type { JazzSession } from "../session/jazz-session.js";
import type { JazzClient } from "./create-jazz-client.js";
import { JazzProvider } from "./provider.js";
import { useAll } from "./use-all.js";

// A hook on a table with end-to-end encrypted columns, under a session that
// starts thin: it stays loading (never an empty list in place of the rows the
// thin client can't read) until the local-first client takes over, then
// shows the rows.

// The DOM environment's WebSocket can't reach the test server; Node's can.
beforeEach(() => vi.stubGlobal("WebSocket", NodeWebSocket));

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  cleanup();
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
  vi.unstubAllGlobals();
});

async function serve() {
  const jazz = await createJazz({
    app,
    permissions,
    store: memory(),
    authenticate: acceptDeviceAccounts,
  }).ready();
  cleanups.push(() => jazz.close());
  const server: Server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  await jazz.attach(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(
    () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  );
  return {
    appId: jazz.appId,
    serverUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}/jazz`,
  };
}

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    read: async () => stored,
    update: async (transform) => void (stored = transform(stored)),
  };
}

/** What the hook rendered: `loading`, or the titles it had. */
const shown: string[] = [];
function Tasks() {
  const { data: tasks, isLoading, error } = useAll(app.tasks);
  const text = error
    ? `error: ${error.message}`
    : isLoading || !tasks
      ? "loading"
      : tasks.map((task) => String(task.title)).join(",");
  shown.push(text);
  return <output data-testid="tasks">{text}</output>;
}

const loadLocalFirst: LocalFirstLoader = async () => ({ createClient: createLocalFirstDb });

async function open(
  host: { appId: string; serverUrl: string },
  device: { accounts: AccountStore; privateStore: ReturnType<typeof memoryPrivateStore> },
  client: LocalFirstLoader,
): Promise<JazzSession<JazzClient>> {
  const session = await createJazzClient({
    scope: host.appId,
    serverUrl: host.serverUrl,
    store: device.accounts,
    driver: { type: "memory" },
    initial: "device",
    encryption: { app, store: device.privateStore },
    client,
  } as never);
  cleanups.push(() => session.close());
  return session;
}

it("a hook on an encrypted table loads while thin, then shows its rows from the local-first client", async () => {
  const host = await serve();
  // An earlier visit, on the local-first client: a project with one task.
  const device = { accounts: memoryStore(), privateStore: memoryPrivateStore() };
  const earlier = await open(host, device, loadLocalFirst);
  await waitFor(() => expect(isThinDb(earlier.getSnapshot().client!.db)).toBe(false), {
    timeout: 20_000,
  });
  const first = earlier.getSnapshot().client!.db;
  expect(await first.encryption.ready({ timeoutMs: 20_000 })).toBe("active");
  const account = earlier.getSnapshot().client!.session!.user!;
  const project = crypto.randomUUID();
  const self = await first.encryption.members.row({ table: "projects", id: project }, account);
  await (
    await first.transaction((tx) => {
      tx.insert(app.projects, { name: "plans", ownerId: account } as never, { id: project });
      tx.insert(app.__e2ee_members, self);
    })
  ).wait({ tier: "global" });
  await first.insert(app.tasks, { projectId: project, title: "hello" } as never).wait({
    tier: "global",
  });
  await earlier.close();
  // This browser forgets the upgrade, so the next visit starts thin again.
  localStorage.clear();

  // This visit starts thin, its loader held.
  let release!: () => void;
  const released = new Promise<void>((resolve) => (release = resolve));
  const session = await open(host, device, async () => {
    await released;
    return { createClient: createLocalFirstDb };
  });
  const db = session.getSnapshot().client!.db;
  expect(isThinDb(db)).toBe(true);

  const view = render(
    <JazzProvider session={session}>
      <Tasks />
    </JazzProvider>,
  );
  // The thin client answers a read meanwhile, so the hook had its chance.
  await act(async () => {
    await db.all(app.projects, { tier: "remote" });
  });
  expect(view.getByTestId("tasks").textContent).toBe("loading");

  await act(async () => {
    release();
    await waitFor(() => expect(isThinDb(db)).toBe(false), { timeout: 20_000 });
  });
  await waitFor(() => expect(view.getByTestId("tasks").textContent).toBe("hello"), {
    timeout: 20_000,
  });
  // Never an empty list or an error in place of the rows.
  expect(shown.filter((text) => text !== "loading" && text !== "hello")).toEqual([]);
}, 90_000);
