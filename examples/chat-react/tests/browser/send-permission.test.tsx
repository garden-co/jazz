/**
 * E2E browser tests for INSERT permission enforcement in private chats.
 *
 * Both hypotheses concern the messages INSERT policy:
 *   WITH CHECK (EXISTS (SELECT FROM chatMembers
 *                       WHERE (chat = @session.__jazz_outer_row.chat)
 *                         AND (userId = @session.user_id)))
 *
 * Hypothesis A — the chat creator (who has a chatMember row from chat
 *   creation) cannot send a message because the EXISTS check in WITH CHECK
 *   fails to find their own membership.
 *
 * Hypothesis B — a user who joined via invite (also a chatMember) cannot
 *   send a message for the same reason.
 *
 *        alice                         bob
 *          │ creates private chat        │
 *          │ inserts chatMember(alice)   │
 *          │ "This is a private chat."  │
 *          │                             │
 *   [Hyp A]│ alice types + sends         │
 *          │ → should succeed            │
 *          │                             │
 *          │ alice generates invite link │
 *          │                    bob follows invite link
 *          │                    InviteHandler inserts chatMember(bob)
 *          │                             │
 *          │              [Hyp B] bob types + sends
 *          │                    → should succeed
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { App } from "../../src/App.js";
import { TEST_PORT, APP_ID } from "./test-constants.js";

// ---------------------------------------------------------------------------
// Helpers (same conventions as chat-app.test.tsx)
// ---------------------------------------------------------------------------

function uniqueDbName(label: string): string {
  return `test-send-perm-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
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

function simulateClick(el: HTMLElement) {
  el.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, cancelable: true }));
  el.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, cancelable: true }));
  el.click();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Send permission — private chat INSERT policy", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  async function mountApp(
    config: {
      dbName?: string;
      serverUrl?: string;
      localAuthMode?: "anonymous" | "demo";
      localAuthToken?: string;
    } = {},
  ): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);
    const r = createRoot(el);
    mounts.push({ root: r, container: el });

    await act(async () => {
      r.render(<App config={{ appId: APP_ID, ...config }} />);
    });

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
    window.location.hash = "";
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

  /**
   * Navigate Alice's app to the Chat List and create a private chat.
   * Returns the chatId extracted from the URL hash.
   */
  async function aliceCreatePrivateChat(aliceContainer: HTMLDivElement): Promise<string> {
    // Open the NavBar menu
    await waitFor(
      () => aliceContainer.querySelector('header [data-slot="dropdown-menu-trigger"]') !== null,
      5000,
      "NavBar menu button should appear",
    );
    const menuButton = aliceContainer.querySelector<HTMLElement>(
      'header [data-slot="dropdown-menu-trigger"]',
    )!;
    await act(async () => simulateClick(menuButton));

    // Click "Chat List"
    await waitFor(
      () =>
        [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].some((i) =>
          i.textContent?.includes("Chat List"),
        ),
      3000,
      "Chat List item should appear",
    );
    const chatListItem = [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].find(
      (i) => i.textContent?.includes("Chat List"),
    ) as HTMLElement;
    await act(async () => simulateClick(chatListItem));

    // Click "New Private Chat"
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

    // Wait for the private chat to load
    await waitFor(
      () => aliceContainer.textContent?.includes("This is a private chat.") ?? false,
      10000,
      "Private chat seed message should appear",
    );

    const match = window.location.hash.match(/#\/chat\/([^/]+)/);
    if (!match) throw new Error(`Could not extract chatId from hash: ${window.location.hash}`);
    return match[1];
  }

  // -------------------------------------------------------------------------
  // Hypothesis A — the chat creator can send a message to their own private chat
  //
  // Alice creates a private chat (chatMember row inserted for her at creation
  // time). She should be able to INSERT a message because the EXISTS policy
  // check should find her chatMember row.
  // -------------------------------------------------------------------------

  it("private chat creator can send a message", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    const aliceContainer = await mountApp({
      dbName: uniqueDbName("alice-a"),
      serverUrl,
      localAuthMode: "demo",
      localAuthToken: `send-perm-alice-a-${Date.now()}`,
    });

    // Alice's app auto-creates a public chat first; navigate to a private one
    await waitFor(
      () => aliceContainer.querySelector("#messageEditor") !== null,
      10000,
      "Alice editor should be visible",
    );

    await aliceCreatePrivateChat(aliceContainer);

    // Alice types and sends a message
    await waitFor(
      () => aliceContainer.querySelector("#messageEditor") !== null,
      5000,
      "Message editor should be visible in private chat",
    );

    const editor = aliceContainer.querySelector<HTMLElement>("#messageEditor")!;
    const sendButton = [...aliceContainer.querySelectorAll("button")].find((b) =>
      b.querySelector(".lucide-send"),
    ) as HTMLElement | undefined;

    await act(async () => typeIntoEditor(editor, "Alice's private message"));
    if (sendButton) await act(async () => simulateClick(sendButton));

    // The message should appear — Alice is a chatMember so INSERT should succeed
    await waitFor(
      () => aliceContainer.textContent?.includes("Alice's private message") ?? false,
      5000,
      "Alice's message should appear in the private chat",
    );
  });

  // -------------------------------------------------------------------------
  // Hypothesis B — a user who joined via invite can send a message
  //
  // Alice creates a private chat and generates an invite link. Bob follows
  // the link — InviteHandler inserts a chatMember row for him. Bob should
  // then be able to INSERT a message via the same EXISTS policy check.
  // -------------------------------------------------------------------------

  it("invited member can send a message", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    // --- Alice: create private chat and generate an invite link -------------
    const aliceContainer = await mountApp({
      dbName: uniqueDbName("alice-b"),
      serverUrl,
      localAuthMode: "demo",
      localAuthToken: `send-perm-alice-b-${Date.now()}`,
    });

    await waitFor(
      () => aliceContainer.querySelector("#messageEditor") !== null,
      10000,
      "Alice editor should be visible",
    );

    await aliceCreatePrivateChat(aliceContainer);

    // Open the ChatSettings sheet via the gear icon in the header
    await waitFor(
      () => aliceContainer.querySelector('[data-testid="chat-header"]') !== null,
      5000,
      "ChatHeader should be visible",
    );

    const gearButton = aliceContainer.querySelector<HTMLElement>(
      '[data-testid="chat-header"] button:has(.lucide-settings)',
    );
    if (!gearButton) throw new Error("Could not find settings gear button");
    await act(async () => simulateClick(gearButton));

    await waitFor(
      () => document.querySelector('[data-slot="sheet-content"]') !== null,
      5000,
      "ChatSettings sheet should open",
    );

    // Click "Invite to chat" in the settings sheet
    const inviteButton = [...document.querySelectorAll('[data-slot="sheet-content"] button')].find(
      (b) => b.textContent?.toLowerCase().includes("invite"),
    ) as HTMLElement;
    if (!inviteButton) throw new Error("Could not find invite button in settings");
    await act(async () => simulateClick(inviteButton));

    await waitFor(
      () => document.querySelector<HTMLInputElement>("input#link") !== null,
      5000,
      "Invite link input should appear",
    );
    const inviteLink = document.querySelector<HTMLInputElement>("input#link")!.value;
    expect(inviteLink).toBeTruthy();

    // Give the server time to persist Alice's chatMember and seed message, then
    // unmount Alice BEFORE setting the invite hash. Both apps share
    // window.location; if Alice is still mounted when the hash changes to the
    // invite URL, her InviteHandler mounts, sees the chat immediately (via
    // createdBy policy), and navigates back to the chat — interrupting Bob's
    // join flow before Bob's chatMember is inserted.
    await new Promise((r) => setTimeout(r, 2000));
    await unmountApp(aliceContainer);

    // --- Bob: follow the invite link ----------------------------------------
    const hashIdx = inviteLink.indexOf("#");
    if (hashIdx !== -1) window.location.hash = inviteLink.substring(hashIdx + 1);

    const bobContainer = await mountApp({
      dbName: uniqueDbName("bob-b"),
      serverUrl,
      localAuthMode: "demo",
      localAuthToken: `send-perm-bob-b-${Date.now()}`,
    });

    // InviteHandler should redirect Bob to the chat after inserting chatMember
    await waitFor(
      () => {
        const text = bobContainer.textContent ?? "";
        return text.includes("This is a private chat.");
      },
      10000,
      "Bob should see the private chat seed message after joining",
    );

    // Bob types and sends a message
    await waitFor(
      () => bobContainer.querySelector("#messageEditor") !== null,
      5000,
      "Message editor should be visible for Bob",
    );

    const editor = bobContainer.querySelector<HTMLElement>("#messageEditor")!;
    const sendButton = [...bobContainer.querySelectorAll("button")].find((b) =>
      b.querySelector(".lucide-send"),
    ) as HTMLElement | undefined;

    await act(async () => typeIntoEditor(editor, "Bob's reply to the private chat"));
    if (sendButton) await act(async () => simulateClick(sendButton));

    // Bob's message should appear — he is a chatMember so INSERT should succeed
    await waitFor(
      () => bobContainer.textContent?.includes("Bob's reply to the private chat") ?? false,
      5000,
      "Bob's message should appear in the private chat",
    );
  });
});
