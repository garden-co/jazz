import React, { useState } from "react";
import { act, cleanup, fireEvent, render, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { WebSocket as NodeWebSocket } from "undici";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb, type LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import { startTestJazz, type TestJazz } from "../testing/index.js";
import { createJazzClient } from "../session/create-jazz-session.js";
import type { JazzSession } from "../session/jazz-session.js";
import type { JazzClient } from "./create-jazz-client.js";
import { JazzProvider } from "./provider.js";
import { useAll } from "./use-all.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) }, { migrations: "none" });
const permissions = definePermissions(app, ({ policy, session }) => {
  policy.notes.allowRead.where({ $createdBy: session.user });
  policy.notes.allowInsert.always();
});
const loadLocalFirst: LocalFirstLoader = async () => ({ createClient: createLocalFirstDb });

// The DOM environment's WebSocket can't reach the test server; Node's can.
beforeEach(() => vi.stubGlobal("WebSocket", NodeWebSocket));

let server: TestJazz | undefined;
let session: JazzSession<JazzClient> | undefined;
afterEach(async () => {
  cleanup();
  vi.unstubAllGlobals();
  await session?.close();
  await server?.stop();
  session = server = undefined;
});

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    read: async () => stored,
    update: async (transform) => void (stored = transform(stored)),
  };
}

const mounts: number[] = [];
const loading: number[] = [];
function Notes() {
  // A component's own state, and what the user is typing, survive the upgrade.
  const [draft, setDraft] = useState("");
  useState(() => mounts.push(1));
  const { data: notes, isLoading } = useAll(app.notes);
  if (isLoading) loading.push(1);
  return (
    <div>
      <input data-testid="draft" value={draft} onChange={(e) => setDraft(e.target.value)} />
      <output data-testid="notes">{notes?.map((note) => note.text).join(",") ?? "loading"}</output>
    </div>
  );
}

it("upgrades to the local-first client under a mounted tree, keeping its state and results", async () => {
  server = await startTestJazz({
    app,
    permissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  const opened = await createJazzClient({
    scope: server.appId,
    serverUrl: server.url,
    store: memoryStore(),
    initial: "device",
    driver: { type: "memory" },
  });
  session = opened;
  const db = opened.getSnapshot().client!.db;
  await db.insert(app.notes, { text: "before" }).wait({ tier: "global" });

  const view = render(
    <JazzProvider session={opened}>
      <Notes />
    </JazzProvider>,
  );
  await waitFor(() => expect(view.getByTestId("notes").textContent).toBe("before"));
  fireEvent.change(view.getByTestId("draft"), { target: { value: "half-typed" } });
  const mountsBefore = mounts.length;
  const loadingBefore = loading.length;

  await act(() => db.upgradeToLocalFirst(loadLocalFirst));
  expect(isThinDb(opened.getSnapshot().client!.db)).toBe(false);

  expect(mounts.length).toBe(mountsBefore);
  expect(loading.length).toBe(loadingBefore);
  expect((view.getByTestId("draft") as HTMLInputElement).value).toBe("half-typed");
  expect(view.getByTestId("notes").textContent).toBe("before");

  await act(async () => {
    await db.insert(app.notes, { text: "after" }).wait({ tier: "global" });
  });
  await waitFor(() =>
    expect(view.getByTestId("notes").textContent?.split(",").sort()).toEqual(["after", "before"]),
  );
  expect(loading.length).toBe(loadingBefore);
}, 60_000);
