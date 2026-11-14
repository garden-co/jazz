import { memoryAccountStore } from "../../../testing/accounts.js";
/**
 * Core browser canary for the React todo app.
 *
 * Mounts the real <App /> component in Chromium and connects two persistent
 * IndexedDB clients to the app's backend. The public serverUrl config is
 * converted by the runtime to the websocket endpoint.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act, useEffect } from "react";
import { App } from "../../src/App.js";
import { app } from "../../schema.js";
import { SCOPE, SERVER_URL } from "./test-constants.js";
import type { DbConfig } from "jazz-tools";

type AccountStore = ReturnType<typeof memoryAccountStore>;
// These tests are about the local-first client (IndexedDB, sync between replicas).
const loadLocalFirst = () => import("jazz-tools/local-first");
import { useDb } from "jazz-tools/react";

type TestWindow = Window & {
  __jazz?: { shutdown(namespace?: string): Promise<void> };
};

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });
  }
  throw new Error(`Timeout: ${message}`);
}

function typeInto(input: HTMLInputElement, value: string) {
  const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")!.set!;
  setter.call(input, value);
  input.dispatchEvent(new Event("input", { bubbles: true }));
}

function todoTitles(el: HTMLDivElement): Array<string | null> {
  return [...el.querySelectorAll("#todo-list li span")].map((span) => span.textContent);
}

function hasTodoTitle(el: HTMLDivElement, title: string): boolean {
  return todoTitles(el).includes(title);
}

function todoItemByTitle(el: HTMLDivElement, title: string): HTMLLIElement | null {
  return (
    [...el.querySelectorAll<HTMLLIElement>("#todo-list li")].find(
      (li) => li.querySelector("span")?.textContent === title,
    ) ?? null
  );
}

async function addTodo(el: HTMLDivElement, title: string): Promise<void> {
  const input = el.querySelector<HTMLInputElement>("input[type='text']")!;
  const form = input.closest("form")!;

  await act(async () => {
    typeInto(input, title);
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
  });
}

async function addTodoAndWaitForLocalDurability(el: HTMLDivElement, title: string): Promise<void> {
  const localWriteDurable = new Promise<void>((resolve) => {
    window.addEventListener("todo-app:local-write-durable", () => resolve(), { once: true });
  });

  await addTodo(el, title);
  await act(async () => {
    await localWriteDurable;
  });
}

function TodosRemoteReadinessProbe({ onSettled }: { onSettled: (error: Error | null) => void }) {
  const db = useDb();
  useEffect(() => {
    // A remote read of the same relation establishes the causal frontier that
    // this browser canary is about. A disjoint sentinel only proved that a
    // websocket existed; it could resolve before the todo subscription had
    // been attached or replayed after a persistent-worker restart.
    void db.all(app.todos, { tier: "remote" }).then(
      () => onSettled(null),
      (error: unknown) => onSettled(error instanceof Error ? error : new Error(String(error))),
    );
  }, [db, onSettled]);
  return null;
}

describe("React Todo App core browser canary", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  async function mountApp(config: {
    appId?: string;
    serverUrl?: string;
    /** The device account's store: mounts sharing one are the same user. */
    store?: AccountStore;
    driver?: DbConfig["driver"];
    onRemoteSettled?: (error: Error | null) => void;
  }): Promise<HTMLDivElement> {
    const { onRemoteSettled, store = memoryAccountStore(), ...dbConfig } = config;
    const appId = dbConfig.appId ?? "test-app";
    const el = document.createElement("div");
    document.body.appendChild(el);
    const r = createRoot(el);
    mounts.push({ root: r, container: el });

    await act(async () => {
      r.render(
        <App config={{ appId, ...dbConfig, store, client: loadLocalFirst, localFirst: "now" }}>
          {onRemoteSettled && <TodosRemoteReadinessProbe onSettled={onRemoteSettled} />}
        </App>,
      );
    });

    await waitFor(
      () => el.querySelector("#todo-list") !== null,
      5000,
      "App should render the todo list",
    );

    return el;
  }

  async function unmountApp(el: HTMLDivElement, dbName?: string): Promise<void> {
    const idx = mounts.findIndex((m) => m.container === el);
    if (idx === -1) return;

    const { root } = mounts[idx];
    await act(async () => {
      await (window as TestWindow).__jazz?.shutdown(dbName);
    });
    await act(async () => root.unmount());
    el.remove();
    mounts.splice(idx, 1);
    await act(async () => {
      await new Promise((r) => setTimeout(r, 200));
    });
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
  });

  it("syncs two persistent IndexedDB app instances through the app's backend", async () => {
    const writerDbName = uniqueDbName("core-writer");
    const readerDbName = uniqueDbName("core-reader");
    let writerRemoteResult: Error | null | undefined;
    let readerRemoteResult: Error | null | undefined;

    const writer = await mountApp({
      appId: SCOPE,
      driver: { type: "persistent", dbName: writerDbName },
      serverUrl: SERVER_URL,
      onRemoteSettled: (error) => {
        writerRemoteResult = error;
      },
    });
    const reader = await mountApp({
      appId: SCOPE,
      driver: { type: "persistent", dbName: readerDbName },
      serverUrl: SERVER_URL,
      onRemoteSettled: (error) => {
        readerRemoteResult = error;
      },
    });

    await waitFor(
      () => writerRemoteResult !== undefined && readerRemoteResult !== undefined,
      15000,
      "both todo subscriptions should complete a remote read before the online write",
    );
    if (writerRemoteResult) throw writerRemoteResult;
    if (readerRemoteResult) throw readerRemoteResult;

    await addTodo(writer, "Core writer todo");
    await waitFor(
      () => hasTodoTitle(reader, "Core writer todo"),
      20000,
      "reader useAll subscription should observe writer create",
    );

    const writerItem = todoItemByTitle(writer, "Core writer todo")!;
    await act(async () => writerItem.querySelector<HTMLInputElement>("input.toggle")!.click());

    await waitFor(
      () => todoItemByTitle(writer, "Core writer todo")?.classList.contains("done") === true,
      3000,
      "writer should render its own update before remount",
    );

    await waitFor(
      () => todoItemByTitle(reader, "Core writer todo")?.classList.contains("done") === true,
      20000,
      "reader useAll subscription should observe writer update",
    );

    await addTodo(reader, "Core reader todo");
    await waitFor(
      () => hasTodoTitle(writer, "Core reader todo"),
      20000,
      "writer useAll subscription should observe reader create",
    );

    const deleteButton = todoItemByTitle(
      writer,
      "Core writer todo",
    )!.querySelector<HTMLButtonElement>(".delete-btn")!;
    await act(async () => deleteButton.click());

    await waitFor(
      () => !hasTodoTitle(reader, "Core writer todo"),
      20000,
      "reader useAll subscription should observe writer delete",
    );
  });

  it("reopens a persistent IndexedDB app instance with DOM-written todos", async () => {
    const dbName = uniqueDbName("core-reopen");
    const title = "Core durable todo";
    const store = memoryAccountStore();

    const firstSession = await mountApp({
      appId: SCOPE,
      driver: { type: "persistent", dbName },
      serverUrl: SERVER_URL,
      store,
    });

    await addTodoAndWaitForLocalDurability(firstSession, title);
    await waitFor(
      () => hasTodoTitle(firstSession, title),
      3000,
      "first session should render the DOM-written todo",
    );

    await unmountApp(firstSession, dbName);

    const secondSession = await mountApp({
      appId: SCOPE,
      driver: { type: "persistent", dbName },
      serverUrl: SERVER_URL,
      store,
    });

    await waitFor(
      () => hasTodoTitle(secondSession, title),
      5000,
      "remounted app should load the locally durable todo from IndexedDB",
    );

    expect(todoTitles(secondSession)).toContain(title);
  });

  it("reopened persistent IndexedDB client catches up a locally durable write after websocket reconnect", async () => {
    const writerDbName = uniqueDbName("core-reconnect-writer");
    const readerDbName = uniqueDbName("core-reconnect-reader");
    const writerStore = memoryAccountStore();
    const readerStore = memoryAccountStore();
    const onlineTitle = "Core reconnect online todo";
    const offlineTitle = "Core reconnect offline todo";
    let writerRemoteResult: Error | null | undefined;
    let readerRemoteResult: Error | null | undefined;

    const writer = await mountApp({
      appId: SCOPE,
      driver: { type: "persistent", dbName: writerDbName },
      serverUrl: SERVER_URL,
      store: writerStore,
      onRemoteSettled: (error) => {
        writerRemoteResult = error;
      },
    });
    const reader = await mountApp({
      appId: SCOPE,
      driver: { type: "persistent", dbName: readerDbName },
      serverUrl: SERVER_URL,
      store: readerStore,
      onRemoteSettled: (error) => {
        readerRemoteResult = error;
      },
    });

    await waitFor(
      () => writerRemoteResult !== undefined && readerRemoteResult !== undefined,
      15000,
      "both todo subscriptions should complete a remote read before the online write",
    );
    if (writerRemoteResult) throw writerRemoteResult;
    if (readerRemoteResult) throw readerRemoteResult;

    await addTodo(writer, onlineTitle);
    await waitFor(
      () => hasTodoTitle(reader, onlineTitle),
      20000,
      "reader should observe the writer before the offline window",
    );

    await unmountApp(reader, readerDbName);

    await addTodoAndWaitForLocalDurability(writer, offlineTitle);
    await waitFor(
      () => hasTodoTitle(writer, offlineTitle),
      3000,
      "writer should render the locally durable todo while reader is offline",
    );

    let remoteResult: Error | null | undefined;
    const reconnectedReader = await mountApp({
      appId: SCOPE,
      driver: { type: "persistent", dbName: readerDbName },
      serverUrl: SERVER_URL,
      store: readerStore,
      onRemoteSettled: (error) => {
        remoteResult = error;
      },
    });
    await waitFor(
      () => remoteResult !== undefined,
      15000,
      "reopened reader should complete a remote todo read before catch-up",
    );
    if (remoteResult) throw remoteResult;

    // The prior online row is already in this reader's IndexedDB store. Check that
    // local rehydration separately, so a later failure is unambiguously the
    // websocket catch-up stage rather than a remount failure.
    await waitFor(
      () => hasTodoTitle(reconnectedReader, onlineTitle),
      5000,
      "reopened reader should rehydrate its known local row before reconnecting",
    );

    await waitFor(
      () => hasTodoTitle(reconnectedReader, offlineTitle),
      30000,
      "reopened reader should catch up the offline-window write after websocket reconnect",
    );

    expect(todoTitles(reconnectedReader)).toEqual(
      expect.arrayContaining([onlineTitle, offlineTitle]),
    );
  }, 100000);
});
