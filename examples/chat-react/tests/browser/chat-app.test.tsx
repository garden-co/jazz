/**
 * E2E browser tests for the React chat app.
 *
 * Mounts the real <App /> component in Chromium via @vitest/browser + playwright.
 * Adapted from Jazz 1 Playwright chat.spec.ts to Jazz 2 Vitest browser tests.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { App } from "../../src/App.js";
import { TEST_PORT, APP_ID, testSecret } from "./test-constants.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

function typeIntoEditor(editorEl: HTMLElement, text: string) {
  const handle = (editorEl as any).__editorHandle;
  if (!handle) throw new Error("No __editorHandle found on #messageEditor");
  handle.insertText(text);
}

/** Simulate a real click (pointerdown → pointerup → click). Radix UI
 *  components open on pointerdown, so a bare `.click()` won't work. */
function simulateClick(el: HTMLElement) {
  el.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, cancelable: true }));
  el.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, cancelable: true }));
  el.click();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Chat App E2E", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  async function mountApp(
    config: {
      appId?: string;
      dbName?: string;
      serverUrl?: string;
      auth?: { localFirstSecret: string };
    } = {},
  ): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);
    const r = createRoot(el);
    mounts.push({ root: r, container: el });

    // Each test gets a unique appId to avoid OPFS lock contention between
    // sequential tests (the previous worker may still be shutting down).
    const appId =
      config.appId ?? `test-chat-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

    await act(async () => {
      r.render(<App config={{ appId, ...config }} />);
    });

    // Wait for the app to initialise and redirect to a chat
    await waitFor(
      () => el.querySelector("#messageEditor") !== null || el.querySelector("article") !== null,
      10000,
      "App should render the message editor or a chat view",
    );

    return el;
  }

  async function unmountApp(el: HTMLDivElement): Promise<void> {
    const idx = mounts.findIndex((m) => m.container === el);
    if (idx === -1) return;
    const { root } = mounts[idx];
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
    // Reset the hash so the next test starts at the root path, which
    // triggers CreateChatRedirect to create a fresh chat.
    window.location.hash = "";
    // Wait for the JazzProvider's async shutdown (worker termination,
    // OPFS lock release) to complete before starting the next test.
    await new Promise((r) => setTimeout(r, 1000));
  });

  // -------------------------------------------------------------------------
  // 1. Initial load: app mounts, creates public chat, shows "Hello world"
  // -------------------------------------------------------------------------

  it("creates a public chat on initial load with seed message", async () => {
    const el = await mountApp({ dbName: uniqueDbName("initial") });

    await waitFor(
      () => el.textContent?.includes("Hello world") ?? false,
      10000,
      "Should show the seed 'Hello world' message",
    );

    expect(el.querySelector("#messageEditor")).toBeTruthy();
  });

  // -------------------------------------------------------------------------
  // 2. Send a message
  // -------------------------------------------------------------------------

  it("sends a message and shows it in the chat", async () => {
    const el = await mountApp({ dbName: uniqueDbName("send") });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null,
      10000,
      "Message editor should be visible",
    );

    const editor = el.querySelector<HTMLElement>("#messageEditor")!;
    const sendButton =
      el.querySelector<HTMLButtonElement>('[data-slot="button"]:has(.lucide-send)') ??
      [...el.querySelectorAll("button")].find((b) => b.querySelector(".lucide-send"));

    await act(async () => {
      typeIntoEditor(editor, "Hello Public 2");
    });

    if (sendButton) {
      await act(async () => simulateClick(sendButton));
    }

    await waitFor(
      () => el.textContent?.includes("Hello Public 2") ?? false,
      5000,
      "Sent message should appear in chat",
    );
  });

  // -------------------------------------------------------------------------
  // 3. React to a message
  // -------------------------------------------------------------------------

  it("adds a reaction to a message", async () => {
    const el = await mountApp({ dbName: uniqueDbName("react") });

    await waitFor(
      () => el.textContent?.includes("Hello world") ?? false,
      10000,
      "Seed message should be visible",
    );

    // Click on the message to open the dropdown menu
    const messageBubble = [...el.querySelectorAll("article")].find((a) =>
      a.textContent?.includes("Hello world"),
    );
    expect(messageBubble).toBeTruthy();

    // DropdownMenuTrigger asChild overrides Item's data-slot to "dropdown-menu-trigger"
    const clickTarget = messageBubble!.querySelector('[data-slot="dropdown-menu-trigger"]');
    expect(clickTarget).toBeTruthy();
    await act(async () => simulateClick(clickTarget as HTMLElement));

    // Wait for dropdown to appear and find the React submenu
    await waitFor(
      () => {
        const items = document.querySelectorAll('[data-slot="dropdown-menu-sub-trigger"]');
        return items.length > 0;
      },
      3000,
      "React submenu trigger should appear",
    );

    // Open the React submenu (Radix uses onPointerMove, not onPointerEnter;
    // clicking the trigger also opens it).
    const reactTrigger = document.querySelector(
      '[data-slot="dropdown-menu-sub-trigger"]',
    ) as HTMLElement;
    expect(reactTrigger).toBeTruthy();
    await act(async () => simulateClick(reactTrigger));

    // Wait for submenu content and click heart
    await waitFor(
      () => {
        const buttons = document.querySelectorAll('[data-slot="dropdown-menu-sub-content"] button');
        return buttons.length > 0;
      },
      3000,
      "Reaction buttons should appear",
    );

    const heartButton = [
      ...document.querySelectorAll('[data-slot="dropdown-menu-sub-content"] button'),
    ].find((b) => b.textContent?.includes("❤️"));
    if (heartButton) {
      await act(async () => simulateClick(heartButton as HTMLElement));
    }

    // Verify the reaction pill appears
    await waitFor(
      () => {
        const article = [...el.querySelectorAll("article")].find((a) =>
          a.textContent?.includes("Hello world"),
        );
        return article?.textContent?.includes("❤️") ?? false;
      },
      5000,
      "Heart reaction pill should appear on message",
    );
  });

  // -------------------------------------------------------------------------
  // 4. Delete a message
  // -------------------------------------------------------------------------

  it("deletes a message via the dropdown menu", async () => {
    const el = await mountApp({ dbName: uniqueDbName("delete") });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null,
      10000,
      "Editor should be visible",
    );

    // Send a message to delete
    const editor = el.querySelector<HTMLElement>("#messageEditor")!;
    const sendButton = [...el.querySelectorAll("button")].find((b) =>
      b.querySelector(".lucide-send"),
    );

    await act(async () => typeIntoEditor(editor, "Message to delete"));
    if (sendButton) {
      await act(async () => simulateClick(sendButton));
    }

    await waitFor(
      () => el.textContent?.includes("Message to delete") ?? false,
      5000,
      "Message should appear",
    );

    // Click on the message
    const messageBubble = [...el.querySelectorAll("article")].find((a) =>
      a.textContent?.includes("Message to delete"),
    );
    const clickTarget = messageBubble?.querySelector('[data-slot="dropdown-menu-trigger"]');
    expect(clickTarget).toBeTruthy();
    await act(async () => simulateClick(clickTarget as HTMLElement));

    // Find and click the Delete menu item
    await waitFor(
      () => {
        const items = document.querySelectorAll('[data-slot="dropdown-menu-item"]');
        return [...items].some((i) => i.textContent?.includes("Delete"));
      },
      3000,
      "Delete menu item should appear",
    );

    const deleteItem = [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].find(
      (i) => i.textContent?.includes("Delete"),
    ) as HTMLElement;
    if (deleteItem) {
      await act(async () => simulateClick(deleteItem));
    }

    // Confirm deletion in the alert dialog
    await waitFor(
      () => document.querySelector('[data-slot="alert-dialog-action"]') !== null,
      3000,
      "Delete confirmation dialog should appear",
    );

    const confirmButton = document.querySelector(
      '[data-slot="alert-dialog-action"]',
    ) as HTMLElement;
    if (confirmButton) {
      await act(async () => simulateClick(confirmButton));
    }

    await waitFor(
      () => !(el.textContent?.includes("Message to delete") ?? false),
      5000,
      "Deleted message should no longer be visible",
    );
  });

  // -------------------------------------------------------------------------
  // 5. Create a public chat via Chat List
  // -------------------------------------------------------------------------

  it("creates a new public chat via the chat list", async () => {
    const el = await mountApp({ dbName: uniqueDbName("newchat") });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null,
      10000,
      "Editor should be visible",
    );

    // Wait for the NavBar to load and open the menu
    await waitFor(
      () => el.querySelector('header [data-slot="dropdown-menu-trigger"]') !== null,
      5000,
      "NavBar menu button should appear",
    );
    const menuButton = el.querySelector<HTMLElement>('header [data-slot="dropdown-menu-trigger"]')!;
    await act(async () => simulateClick(menuButton));

    await waitFor(
      () => {
        const items = document.querySelectorAll('[data-slot="dropdown-menu-item"]');
        return [...items].some((i) => i.textContent?.includes("Chat List"));
      },
      3000,
      "Chat List menu item should appear",
    );

    const chatListItem = [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].find(
      (i) => i.textContent?.includes("Chat List"),
    ) as HTMLElement;
    if (chatListItem) {
      await act(async () => simulateClick(chatListItem));
    }

    // Click "New Chat" button
    await waitFor(
      () => {
        const buttons = el.querySelectorAll("button");
        return [...buttons].some((b) => b.textContent?.includes("New Chat"));
      },
      5000,
      "New Chat button should appear",
    );

    const newChatButton = [...el.querySelectorAll("button")].find(
      (b) => b.textContent?.includes("New Chat") && !b.textContent?.includes("Private"),
    ) as HTMLElement;
    if (newChatButton) {
      await act(async () => simulateClick(newChatButton));
    }

    // Should redirect to the new chat and show "Hello world"
    await waitFor(
      () => el.textContent?.includes("Hello world") ?? false,
      10000,
      "New chat should show seed message",
    );
  });

  // -------------------------------------------------------------------------
  // 6. Private access denied (ReBAC enforced)
  //
  //    User A creates a private chat and sends "Secret Data".
  //    User B mounts and navigates to the same chat URL.
  //    User B should see "You don't have access" and NOT see "Secret Data".
  // -------------------------------------------------------------------------

  // -------------------------------------------------------------------------
  // Shared setup: User A creates a private chat with a secret message,
  // then User B (a non-member) navigates to the same chat URL.
  // -------------------------------------------------------------------------

  async function setupPrivateChatAccess(): Promise<{
    bobContainer: HTMLDivElement;
  }> {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    // --- User A: create a private chat with a secret message ----------------
    const aliceContainer = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("access-a"),
      serverUrl,
      auth: { localFirstSecret: await testSecret(`chat-access-user-a-${Date.now()}`) },
    });

    await waitFor(
      () => aliceContainer.querySelector("#messageEditor") !== null,
      10000,
      "User A editor should be visible",
    );

    // Navigate to chat list and create a private chat
    await waitFor(
      () => aliceContainer.querySelector('header [data-slot="dropdown-menu-trigger"]') !== null,
      5000,
      "NavBar menu button should appear for user A",
    );
    const aliceMenuButton = aliceContainer.querySelector<HTMLElement>(
      'header [data-slot="dropdown-menu-trigger"]',
    )!;
    await act(async () => simulateClick(aliceMenuButton));

    await waitFor(
      () =>
        [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].some((i) =>
          i.textContent?.includes("Chat List"),
        ),
      3000,
      "Chat List menu should appear for user A",
    );

    const aliceChatListItem = [
      ...document.querySelectorAll('[data-slot="dropdown-menu-item"]'),
    ].find((i) => i.textContent?.includes("Chat List")) as HTMLElement;
    await act(async () => simulateClick(aliceChatListItem));

    await waitFor(
      () =>
        [...aliceContainer.querySelectorAll("button")].some((b) =>
          b.textContent?.includes("New Private Chat"),
        ),
      5000,
      "New Private Chat button should appear",
    );

    const privateChatButton = [...aliceContainer.querySelectorAll("button")].find((b) =>
      b.textContent?.includes("New Private Chat"),
    ) as HTMLElement;
    await act(async () => simulateClick(privateChatButton));

    await waitFor(
      () => aliceContainer.querySelector("#messageEditor") !== null,
      10000,
      "Private chat editor should appear for user A",
    );

    // Send the secret message
    const aliceEditor = aliceContainer.querySelector<HTMLElement>("#messageEditor")!;
    const aliceSendButton = [...aliceContainer.querySelectorAll("button")].find((b) =>
      b.querySelector(".lucide-send"),
    );

    await act(async () => typeIntoEditor(aliceEditor, "Secret Data"));
    if (aliceSendButton) {
      await act(async () => simulateClick(aliceSendButton));
    }

    await waitFor(
      () => aliceContainer.textContent?.includes("Secret Data") ?? false,
      5000,
      "Secret message should appear for user A",
    );

    // Capture the private chat URL
    const privateChatHash = window.location.hash;

    // Give the server time to persist
    await new Promise((r) => setTimeout(r, 500));

    // Unmount user A
    await unmountApp(aliceContainer);

    // --- User B: try to access the same private chat ------------------------
    window.location.hash = privateChatHash;

    const bobContainer = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("access-b"),
      serverUrl,
      auth: { localFirstSecret: await testSecret(`chat-access-user-b-${Date.now()}`) },
    });

    // Wait for sync to settle so Bob has whatever data the server delivers
    await new Promise((r) => setTimeout(r, 3000));

    return { bobContainer };
  }

  // -------------------------------------------------------------------------
  // Message ordering
  // -------------------------------------------------------------------------

  it("shows messages in reverse-chronological order (newest first in DOM)", async () => {
    // createdAt is second-granularity (Math.floor(Date.now() / 1000)), so we
    // wait >1 s between sends to guarantee distinct timestamps.
    //
    // The ChatView renders with flex-col-reverse, so DOM order is newest-first:
    //   DOM[0] = msg2  (sent last, highest createdAt)
    //   DOM[1] = msg1
    //   DOM[2] = Hello world  (seed, oldest)
    const el = await mountApp({ dbName: uniqueDbName("ordering") });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null,
      10000,
      "Message editor should be visible",
    );

    const editor = el.querySelector<HTMLElement>("#messageEditor")!;
    const sendButton = [...el.querySelectorAll("button")].find((b) =>
      b.querySelector(".lucide-send"),
    );

    for (const text of ["msg1", "msg2"]) {
      await act(async () => typeIntoEditor(editor, text));
      if (sendButton) await act(async () => simulateClick(sendButton));
      await waitFor(
        () => el.textContent?.includes(text) ?? false,
        5000,
        `Message "${text}" should appear`,
      );
      // Ensure the next message gets a strictly greater createdAt second.
      await new Promise((r) => setTimeout(r, 1100));
    }

    const articleTexts = [...el.querySelectorAll("article")].map((a) => a.textContent ?? "");

    const msg2Idx = articleTexts.findIndex((t) => t.includes("msg2"));
    const msg1Idx = articleTexts.findIndex((t) => t.includes("msg1"));

    expect(msg2Idx).toBeGreaterThanOrEqual(0);
    expect(msg2Idx).toBeLessThan(msg1Idx);
  });

  // -------------------------------------------------------------------------
  // Canvas insertion does not corrupt existing messages
  //
  //   Regression: inserting a canvas caused a column-decoder mismatch that
  //   made every message render garbage (e.g. "<Hello w" instead of "Hello
  //   world").  Inserting a canvas must leave existing message text intact.
  // -------------------------------------------------------------------------

  it("inserting a canvas does not corrupt existing messages", async () => {
    const el = await mountApp({ dbName: uniqueDbName("canvas-corruption") });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null,
      10000,
      "Editor should be visible",
    );

    // Send a plain-text message first
    const editor = el.querySelector<HTMLElement>("#messageEditor")!;
    const sendButton = [...el.querySelectorAll("button")].find((b) =>
      b.querySelector(".lucide-send"),
    );
    await act(async () => typeIntoEditor(editor, "msg1"));
    if (sendButton) await act(async () => simulateClick(sendButton));

    await waitFor(
      () => el.textContent?.includes("msg1") ?? false,
      5000,
      "msg1 should appear before canvas",
    );

    // Insert a canvas
    const plusButton =
      el.querySelector<HTMLElement>("button:has(.lucide-plus)") ??
      [...el.querySelectorAll("button")].find((b) => b.querySelector(".lucide-plus"));
    expect(plusButton).toBeTruthy();
    await act(async () => simulateClick(plusButton as HTMLElement));

    await waitFor(
      () =>
        [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].some((i) =>
          i.textContent?.toLowerCase().includes("canvas"),
        ),
      3000,
      "Canvas menu item should appear",
    );

    const canvasItem = [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].find(
      (i) => i.textContent?.toLowerCase().includes("canvas"),
    ) as HTMLElement;
    await act(async () => simulateClick(canvasItem));

    await waitFor(
      () =>
        el.querySelector('[data-testid="canvas"]') !== null || el.querySelector("canvas") !== null,
      5000,
      "Canvas element should appear",
    );

    // msg1 must still render with the correct text — not a UUID, boolean, or
    // other column-shifted garbage value.
    const articles = [...el.querySelectorAll("article")];
    const msg1Article = articles.find((a) => a.textContent?.includes("msg1"));
    expect(msg1Article).toBeTruthy();

    const proseEl = msg1Article!.querySelector(".prose");
    const renderedText = (proseEl?.textContent ?? msg1Article!.textContent ?? "").trim();
    expect(renderedText).toContain("msg1");
    // A UUID or boolean in place of the text would not contain "msg1"
    // and would look like "019c…" or "false" — the above assertion covers both.
  });

  it("denies access and hides messages for non-members of a private chat", async () => {
    const { bobContainer } = await setupPrivateChatAccess();

    // The secret message should NOT be visible to a non-member
    expect(bobContainer.textContent?.includes("Secret Data")).toBeFalsy();

    // Non-members should see an access denied indicator
    const text = bobContainer.textContent?.toLowerCase() ?? "";
    const hasAccessError =
      text.includes("access") ||
      text.includes("permission") ||
      text.includes("not a member") ||
      text.includes("denied");
    expect(hasAccessError).toBeTruthy();
  });
});
