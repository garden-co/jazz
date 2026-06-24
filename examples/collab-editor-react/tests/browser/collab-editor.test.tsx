/**
 * Browser tests for the collaborative editor example.
 *
 * The core claim of this example is: **Jazz is the durable sync backbone for a
 * Yjs document.** These tests verify that claim end to end against a real local
 * Jazz server (started in global-setup):
 *
 *  1. Editing through the real <App /> + Monaco persists the document and it is
 *     restored after the app is unmounted and remounted (OPFS + server).
 *  2. Two independent clients editing the same room converge — a Yjs edit on one
 *     client's document surfaces on the other, carried entirely by Jazz rows.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act, useEffect, useState } from "react";
import * as monaco from "monaco-editor";
import { nanoid } from "nanoid";
import { JazzProvider, useDb, useSession } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { App } from "../../src/App.js";
import { useJazzYjsDocument } from "../../src/hooks/useJazzYjsDocument.js";
import { app } from "../../schema.js";
import { TEST_PORT, APP_ID, ADMIN_SECRET } from "./test-constants.js";
import type * as Y from "yjs";

const SERVER_URL = `http://127.0.0.1:${TEST_PORT}`;
// Two distinct, valid local-first secrets => two distinct users.
const SECRET_A = "Tb9eLjnS22z-_s9FK0EtiFIIRDe4EAygLAdni55RvAs";
const SECRET_B = "VDOGX2nez-5T9Lgk4VfYMT33Qsa6J4loRAoKLZpvxBg";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Timeout: ${message}`);
}

async function mount(node: React.ReactNode): Promise<{ root: Root; el: HTMLDivElement }> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });
  await act(async () => {
    root.render(node);
  });
  return { root, el };
}

async function unmount(root: Root): Promise<void> {
  const idx = mounts.findIndex((m) => m.root === root);
  await act(async () => root.unmount());
  if (idx !== -1) {
    mounts[idx].container.remove();
    mounts.splice(idx, 1);
  }
  // Give OPFS handles / workers time to release.
  await new Promise((r) => setTimeout(r, 250));
}

afterEach(async () => {
  for (const { root, container } of mounts) {
    try {
      await act(async () => root.unmount());
    } catch {
      /* best effort */
    }
    container.remove();
  }
  mounts.length = 0;
  for (const model of monaco.editor.getModels()) model.dispose();
  history.replaceState({}, "", "/");
});

// ---------------------------------------------------------------------------
// 1. Persistence through the real app + Monaco
// ---------------------------------------------------------------------------

describe("collab editor — persistence", () => {
  it("persists the document through Monaco and restores it after remount", async () => {
    const dbName = uniqueDbName("persist");
    const config: Partial<DbConfig> = {
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      auth: { localFirstSecret: SECRET_A },
      driver: { type: "persistent", dbName },
    };

    // --- session 1: create a room, type into the editor ---
    history.replaceState({}, "", "/");
    const { root: root1 } = await mount(<App config={config} />);

    await waitFor(
      () => document.querySelector<HTMLButtonElement>("[data-testid='new-room']") !== null,
      8000,
      "dashboard should render",
    );

    await act(async () => {
      document.querySelector<HTMLButtonElement>("[data-testid='new-room']")!.click();
    });

    await waitFor(() => location.pathname.startsWith("/r/"), 8000, "should navigate to a room");
    const shareToken = location.pathname.replace("/r/", "");

    await waitFor(() => monaco.editor.getModels().length > 0, 10000, "monaco model should mount");
    const model = monaco.editor.getModels()[0]!;

    // y-monaco initializes the model from the (empty) Y.Text when its binding
    // attaches. Wait for the binding before editing, otherwise it would clobber
    // the edit back to empty.
    await new Promise((r) => setTimeout(r, 1000));
    await act(async () => {
      model.setValue("hello from jazz");
    });
    await waitFor(
      () => model.getValue() === "hello from jazz",
      3000,
      "edit should stick once the Yjs binding is attached",
    );

    // Let the Yjs update persist to the server (tier "edge") and a snapshot form.
    await new Promise((r) => setTimeout(r, 2500));

    await unmount(root1);
    expect(monaco.editor.getModels().length).toBe(0);

    // --- session 2: reopen the same room from a fresh app instance ---
    history.replaceState({}, "", `/r/${shareToken}`);
    await mount(<App config={config} />);

    await waitFor(() => monaco.editor.getModels().length > 0, 10000, "monaco should remount");
    const restored = monaco.editor.getModels()[0]!;

    await waitFor(
      () => restored.getValue() === "hello from jazz",
      10000,
      "document should be restored from Jazz storage",
    );
  });
});

// ---------------------------------------------------------------------------
// 2. Two clients converge through the server
// ---------------------------------------------------------------------------

type DocHandle = { ydoc: Y.Doc };

/**
 * Minimal harness around the real `useJazzYjsDocument` hook. Creates a room when
 * none is provided, then exposes the bound Y.Doc once the hook is ready. No
 * Monaco — we drive and read the Y.Doc directly so two clients can coexist on
 * one page without sharing Monaco's global model registry.
 */
function DocClient(props: {
  providedRoomId?: string;
  onRoom?: (roomId: string) => void;
  onReady: (handle: DocHandle) => void;
}) {
  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const [roomId, setRoomId] = useState<string | null>(props.providedRoomId ?? null);

  useEffect(() => {
    if (props.providedRoomId || roomId || !sessionUserId) return;
    const now = new Date();
    const { value: room } = db.insert(app.rooms, {
      shareToken: nanoid(),
      title: "Untitled",
      editorLanguage: "plaintext",
      creator_session_user_id: sessionUserId,
      createdAt: now,
    });
    db.insert(app.roomParticipants, {
      room_id: room.id,
      session_user_id: sessionUserId,
      lastAccessedAt: now,
    });
    setRoomId(room.id);
    props.onRoom?.(room.id);
  }, [db, props, roomId, sessionUserId]);

  const { ydoc, isReady } = useJazzYjsDocument({ roomId });

  useEffect(() => {
    if (isReady) props.onReady({ ydoc });
  }, [isReady, props, ydoc]);

  return null;
}

function clientConfig(secret: string): DbConfig {
  return {
    appId: APP_ID,
    env: "dev",
    userBranch: "main",
    serverUrl: SERVER_URL,
    secret,
    driver: { type: "memory" },
  };
}

describe("collab editor — multi-client convergence", () => {
  it("syncs a Yjs edit from one client to another through Jazz", async () => {
    let roomId: string | null = null;
    let docA: DocHandle | null = null;
    let docB: DocHandle | null = null;

    // Client A creates the room and binds a document.
    await mount(
      <JazzProvider config={clientConfig(SECRET_A)} fallback={null}>
        <DocClient
          onRoom={(id) => {
            roomId = id;
          }}
          onReady={(handle) => {
            docA = handle;
          }}
        />
      </JazzProvider>,
    );

    await waitFor(() => roomId !== null && docA !== null, 12000, "client A should be ready");

    // Client B joins the same room.
    await mount(
      <JazzProvider config={clientConfig(SECRET_B)} fallback={null}>
        <DocClient
          providedRoomId={roomId!}
          onReady={(handle) => {
            docB = handle;
          }}
        />
      </JazzProvider>,
    );

    await waitFor(() => docB !== null, 12000, "client B should be ready");

    // Edit on A's document...
    await act(async () => {
      docA!.ydoc.getText("monaco").insert(0, "hello world");
    });

    // ...and expect it to converge on B, carried entirely by Jazz rows.
    await waitFor(
      () => docB!.ydoc.getText("monaco").toString() === "hello world",
      15000,
      "edit on client A should converge on client B",
    );

    expect(docB!.ydoc.getText("monaco").toString()).toBe("hello world");
  });
});
