/**
 * E2E browser tests for the invite/join flow.
 *
 * Tests that invite links allow users to join private chats.
 * Adapted from Jazz 1 Playwright invite.spec.ts.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { App } from "../../src/App.js";
import { TEST_PORT, APP_ID, testSecret } from "./test-constants.js";
import { resetProfileGuard } from "../../src/hooks/useMyProfile.js";

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

function simulateClick(el: HTMLElement) {
  el.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, cancelable: true }));
  el.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, cancelable: true }));
  el.click();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Invite Flow E2E", () => {
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

    const appId =
      config.appId ?? `test-invite-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

    await act(async () => {
      r.render(<App config={{ appId, ...config }} />);
    });

    await waitFor(
      () =>
        el.querySelector("#messageEditor") !== null ||
        el.querySelector("article") !== null ||
        el.querySelector("#joining-chat") !== null,
      10000,
      "App should render",
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
    resetProfileGuard();
    for (const { root, container } of mounts) {
      try {
        await act(async () => root.unmount());
      } catch {
        /* best effort */
      }
      container.remove();
    }
    mounts.length = 0;
    window.location.hash = "";
  });

  // -------------------------------------------------------------------------
  // 1. Invite link flow
  //
  //    User A creates a private chat, sends a secret message, generates an
  //    invite link. User B follows the invite link and gains access.
  // -------------------------------------------------------------------------

  it("allows a user to join a private chat via invite link", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const randomSecret = `Secret-${Math.random().toString(36).substring(7)}`;
    let inviteLink = "";

    // --- User A: create private chat and generate invite --------------------
    const aliceContainer = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("invite-a"),
      serverUrl,
      auth: { localFirstSecret: await testSecret(`invite-user-a-${Date.now()}`) },
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
      "NavBar menu button should appear",
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
      "Chat List menu should appear",
    );

    const chatListItem = [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].find(
      (i) => i.textContent?.includes("Chat List"),
    ) as HTMLElement;
    await act(async () => simulateClick(chatListItem));

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
      "Private chat editor should appear",
    );

    // Send the secret message
    const aliceEditor = aliceContainer.querySelector<HTMLElement>("#messageEditor")!;
    const aliceSendButton = [...aliceContainer.querySelectorAll("button")].find((b) =>
      b.querySelector(".lucide-send"),
    );

    await act(async () => typeIntoEditor(aliceEditor, randomSecret));
    if (aliceSendButton) {
      await act(async () => simulateClick(aliceSendButton));
    }

    await waitFor(
      () => aliceContainer.textContent?.includes(randomSecret) ?? false,
      5000,
      "Secret message should appear for user A",
    );

    // Open the ChatSettings sheet via the gear icon in the header
    await waitFor(
      () => aliceContainer.querySelector('[data-testid="chat-header"]') !== null,
      5000,
      "ChatHeader should be visible",
    );

    const gearButton = aliceContainer.querySelector<HTMLElement>(
      '[data-testid="chat-header"] button:has(.lucide-settings)',
    );
    expect(gearButton).toBeTruthy();
    await act(async () => simulateClick(gearButton!));

    await waitFor(
      () => document.querySelector('[data-slot="sheet-content"]') !== null,
      5000,
      "ChatSettings sheet should open",
    );

    // Click "Invite to chat" in the settings sheet
    const inviteButton = [...document.querySelectorAll('[data-slot="sheet-content"] button')].find(
      (b) => b.textContent?.toLowerCase().includes("invite"),
    ) as HTMLElement;
    expect(inviteButton).toBeTruthy();
    await act(async () => simulateClick(inviteButton));

    // Wait for the share modal and read the invite link
    await waitFor(
      () => document.querySelector<HTMLInputElement>("input#link") !== null,
      5000,
      "Invite link input should appear",
    );

    const linkInput = document.querySelector<HTMLInputElement>("input#link")!;
    inviteLink = linkInput.value;
    expect(inviteLink).toContain("/invite/");

    // Close the share modal
    const doneButton = [...document.querySelectorAll("button")].find((b) =>
      b.textContent?.includes("Done"),
    ) as HTMLElement;
    if (doneButton) {
      await act(async () => simulateClick(doneButton));
    }

    // Give the server time to persist
    await new Promise((r) => setTimeout(r, 500));

    // Unmount user A
    await unmountApp(aliceContainer);

    // --- User B: follow the invite link and verify access -------------------

    // Extract the hash portion from the invite link
    const hashIdx = inviteLink.indexOf("#");
    if (hashIdx !== -1) {
      window.location.hash = inviteLink.substring(hashIdx + 1);
    }

    const bobContainer = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("invite-b"),
      serverUrl,
      auth: { localFirstSecret: await testSecret(`invite-user-b-${Date.now()}`) },
    });

    // User B should see the secret message after joining via invite
    await waitFor(
      () => bobContainer.textContent?.includes(randomSecret) ?? false,
      10000,
      "User B should see the secret message after following invite link",
    );
  });
});
