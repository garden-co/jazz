/**
 * Core browser canary for the React todo app.
 *
 * Mounts the real <App /> component in Chromium and connects two persistent
 * OPFS clients to one local Jazz server. The public serverUrl config is
 * converted by the runtime to the websocket endpoint.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { App } from "../../src/App.js";
import { APP_ID, ADMIN_SECRET, SERVER_URL } from "./test-constants.js";
import type { DbConfig } from "jazz-tools";

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
    await new Promise((r) => setTimeout(r, 50));
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
  await localWriteDurable;
}

describe("React Todo App core browser canary", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  async function mountApp(config: {
    appId?: string;
    serverUrl?: string;
    secret?: string;
    adminSecret?: string;
    driver?: DbConfig["driver"];
  }): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);
    const r = createRoot(el);
    mounts.push({ root: r, container: el });

    await act(async () => {
      r.render(<App config={{ appId: config.appId ?? "test-app", ...config }} />);
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
    await (window as TestWindow).__jazz?.shutdown(dbName);
    await act(async () => root.unmount());
    el.remove();
    mounts.splice(idx, 1);
    await new Promise((r) => setTimeout(r, 200));
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

  it("syncs two persistent OPFS app instances through one core server", async () => {
    const writerDbName = uniqueDbName("core-writer");
    const readerDbName = uniqueDbName("core-reader");

    const writer = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: writerDbName },
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      secret: "Tb9eLjnS22z-_s9FK0EtiFIIRDe4EAygLAdni55RvAs",
    });
    const reader = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: readerDbName },
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      secret: "VDOGX2nez-5T9Lgk4VfYMT33Qsa6J4loRAoKLZpvxBg",
    });

    await new Promise((r) => setTimeout(r, 750));

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

  it("reopens a persistent OPFS app instance with DOM-written todos", async () => {
    const dbName = uniqueDbName("core-reopen");
    const title = "Core durable todo";

    const firstSession = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName },
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      secret: "GWA1Dzw4x_QVSAKK3_i0U4MlfJBdYlG3jOwAK_rLx28",
    });

    await addTodoAndWaitForLocalDurability(firstSession, title);
    await waitFor(
      () => hasTodoTitle(firstSession, title),
      3000,
      "first session should render the DOM-written todo",
    );

    await unmountApp(firstSession, dbName);

    const secondSession = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName },
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      secret: "GWA1Dzw4x_QVSAKK3_i0U4MlfJBdYlG3jOwAK_rLx28",
    });

    await waitFor(
      () => hasTodoTitle(secondSession, title),
      5000,
      "remounted app should load the locally durable todo from OPFS",
    );

    expect(todoTitles(secondSession)).toContain(title);
  });

  it("reopened persistent OPFS client catches up a locally durable write after websocket reconnect", async () => {
    const writerDbName = uniqueDbName("core-reconnect-writer");
    const readerDbName = uniqueDbName("core-reconnect-reader");
    const writerSecret = "71E6G0xpMXIiQ_dFv6tCLVCEt33kivVHtQ7FD-fkYlc";
    const readerSecret = "Hw5MHsjqxaP82KsnDXOB9V_7bRWGip1wDRY70fVr8Z8";
    const onlineTitle = "Core reconnect online todo";
    const offlineTitle = "Core reconnect offline todo";

    const writer = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: writerDbName },
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      secret: writerSecret,
    });
    const reader = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: readerDbName },
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      secret: readerSecret,
    });

    await new Promise((r) => setTimeout(r, 750));

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

    const reconnectedReader = await mountApp({
      appId: APP_ID,
      driver: { type: "persistent", dbName: readerDbName },
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      secret: readerSecret,
    });
    await waitFor(
      () => hasTodoTitle(reconnectedReader, offlineTitle),
      20000,
      "reopened reader should catch up the offline-window write after websocket reconnect",
    );

    expect(todoTitles(reconnectedReader)).toEqual(
      expect.arrayContaining([onlineTitle, offlineTitle]),
    );
  });
});
