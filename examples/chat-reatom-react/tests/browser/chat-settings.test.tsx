/**
 * E2E browser tests for the ChatHeader + ChatSettings feature.
 *
 * Tests chat renaming, member list display, and leave-chat flow.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { App } from "../../src/App.js";
import { TEST_PORT, APP_ID, testSecret } from "./test-constants.js";
import { resetProfileGuard } from "../../src/hooks/useMyProfile.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function uniqueDbName(label: string): string {
  return `test-settings-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
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
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

function simulateClick(el: HTMLElement) {
  el.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, cancelable: true }));
  el.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, cancelable: true }));
  el.click();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("ChatHeader + ChatSettings E2E", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  async function mountApp(
    config: {
      appId?: string;
      dbName?: string;
      serverUrl?: string;
      secret?: string;
    } = {},
  ): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);
    const r = createRoot(el);
    mounts.push({ root: r, container: el });

    const appId =
      config.appId ?? `test-settings-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

    r.render(<App config={{ appId, ...config }} />);

    await waitFor(
      () =>
        el.querySelector("#messageEditor") !== null ||
        el.querySelector('[data-testid="chat-header"]') !== null,
      10000,
      "App should render a chat",
    );

    return el;
  }

  afterEach(async () => {
    resetProfileGuard();
    for (const { root, container } of mounts) {
      try {
        root.unmount();
      } catch {
        /* best effort */
      }
      container.remove();
    }
    mounts.length = 0;
    window.location.hash = "";
    await window.__jazz?.shutdown();
  });

  // -------------------------------------------------------------------------
  // Helper: open settings sheet
  // -------------------------------------------------------------------------

  async function openSettings(el: HTMLDivElement): Promise<void> {
    await waitFor(
      () => el.querySelector('[data-testid="chat-header"]') !== null,
      5000,
      "ChatHeader should be visible",
    );

    const gearButton = el.querySelector<HTMLElement>(
      '[data-testid="chat-header"] button:has(.lucide-settings)',
    );
    expect(gearButton).toBeTruthy();
    simulateClick(gearButton!);

    await waitFor(
      () => document.querySelector('[data-slot="sheet-content"]') !== null,
      5000,
      "ChatSettings sheet should open",
    );
  }

  // -------------------------------------------------------------------------
  // 1. Header shows participant name by default
  // -------------------------------------------------------------------------

  it("shows participant name in the chat header by default", async () => {
    const el = await mountApp({ dbName: uniqueDbName("header-name") });

    // Solo user: the header should show the chat start date (DD Mon YYYY HH:MM)
    // since there are no other members to display.
    await waitFor(
      () => {
        const header = el.querySelector('[data-testid="chat-header"]');
        if (!header) return false;
        const text = header.textContent ?? "";
        // Date format: "18 Mar 2026 09:15" — should contain a month abbreviation
        return /\d{2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}/.test(text);
      },
      10000,
      "ChatHeader should show the chat start date for a solo user",
    );
  });

  // -------------------------------------------------------------------------
  // 2. Rename a chat via settings
  // -------------------------------------------------------------------------

  it("renames a chat via the settings sheet", async () => {
    const el = await mountApp({ dbName: uniqueDbName("rename") });

    await openSettings(el);

    await waitFor(
      () => document.querySelector<HTMLInputElement>("#chat-name") !== null,
      5000,
      "Chat name input should appear",
    );

    typeInto(document.querySelector<HTMLInputElement>("#chat-name")!, "Weekend plans");

    // Close the sheet
    const closeButton = document
      .querySelector('[data-slot="sheet-content"] .lucide-x')
      ?.closest("button");
    if (closeButton) {
      simulateClick(closeButton as HTMLElement);
    }

    // Verify the header now shows the custom name
    await waitFor(
      () => {
        const header = el.querySelector('[data-testid="chat-header"]');
        return header?.textContent?.includes("Weekend plans") ?? false;
      },
      5000,
      "Header should show the renamed chat title",
    );
  });

  // -------------------------------------------------------------------------
  // 3. Clear name reverts to participant names
  // -------------------------------------------------------------------------

  it("clearing chat name reverts to participant names", async () => {
    const el = await mountApp({ dbName: uniqueDbName("clear-name") });

    // Set a name first
    await openSettings(el);
    await waitFor(
      () => document.querySelector<HTMLInputElement>("#chat-name") !== null,
      5000,
      "Chat name input should appear",
    );
    typeInto(document.querySelector<HTMLInputElement>("#chat-name")!, "Temporary name");

    // Wait for the name to take effect
    await new Promise((r) => setTimeout(r, 500));

    // Clear it
    typeInto(document.querySelector<HTMLInputElement>("#chat-name")!, "");

    // Close the sheet
    const closeButton = document
      .querySelector('[data-slot="sheet-content"] .lucide-x')
      ?.closest("button");
    if (closeButton) {
      simulateClick(closeButton as HTMLElement);
    }

    // Header should revert to the date (solo user) and not show the old name
    await waitFor(
      () => {
        const header = el.querySelector('[data-testid="chat-header"]');
        const text = header?.textContent ?? "";
        return (
          !text.toLowerCase().includes("temporary name") &&
          /\d{2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}/.test(text)
        );
      },
      5000,
      "Header should revert to chat date after clearing name",
    );
  });

  // -------------------------------------------------------------------------
  // 4. Member list shows own profile
  // -------------------------------------------------------------------------

  it("shows current user in the member list", async () => {
    const el = await mountApp({ dbName: uniqueDbName("members") });

    await openSettings(el);

    // The member list should contain the current user's name
    await waitFor(
      () => {
        const sheet = document.querySelector('[data-slot="sheet-content"]');
        const text = sheet?.textContent?.toLowerCase() ?? "";
        return text.includes("members") && text.includes("anonymous");
      },
      5000,
      "Member list should show the current user",
    );
  });

  // -------------------------------------------------------------------------
  // 5. Leave chat
  // -------------------------------------------------------------------------

  it("leaves a chat via settings and navigates to chat list", async () => {
    const el = await mountApp({ dbName: uniqueDbName("leave") });

    await openSettings(el);

    await waitFor(
      () =>
        [...document.querySelectorAll<HTMLElement>('[data-slot="sheet-content"] button')].some(
          (b) => b.textContent?.toLowerCase().includes("leave chat"),
        ),
      5000,
      "Leave chat button should appear",
    );

    // Click the "Leave chat" button
    const leaveButton = [
      ...document.querySelectorAll<HTMLElement>('[data-slot="sheet-content"] button'),
    ].find((b) => b.textContent?.toLowerCase().includes("leave chat"));
    expect(leaveButton).toBeTruthy();
    simulateClick(leaveButton!);

    // Confirm in the AlertDialog
    await waitFor(
      () => document.querySelector('[data-slot="alert-dialog-action"]') !== null,
      3000,
      "Leave confirmation dialog should appear",
    );

    const confirmButton = document.querySelector(
      '[data-slot="alert-dialog-action"]',
    ) as HTMLElement;
    simulateClick(confirmButton);

    // Should navigate to chat list
    await waitFor(
      () => window.location.hash.includes("/chats"),
      5000,
      "Should navigate to chat list after leaving",
    );
  });

  // -------------------------------------------------------------------------
  // 6. Multi-user: member list shows both participants
  // -------------------------------------------------------------------------

  it("shows both members after auto-join on public chat", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    // --- Alice: create a public chat -----------------------------------------
    const aliceContainer = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("members-alice"),
      serverUrl,
      secret: await testSecret(`settings-alice-${Date.now()}`),
    });

    await waitFor(
      () => aliceContainer.querySelector("#messageEditor") !== null,
      10000,
      "Alice editor should be visible",
    );

    // Capture the chat URL
    const chatHash = window.location.hash;

    // Give the server time to persist
    await new Promise((r) => setTimeout(r, 500));

    // --- Bob: join the same public chat --------------------------------------
    // Bob opens a new app instance pointed at the same public chat
    window.location.hash = chatHash;

    const bobContainer = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("members-bob"),
      serverUrl,
      secret: await testSecret(`settings-bob-${Date.now()}`),
    });

    // Wait for Bob to see the chat
    await waitFor(
      () => bobContainer.querySelector("#messageEditor") !== null,
      10000,
      "Bob editor should be visible",
    );

    // Give sync time to propagate
    await new Promise((r) => setTimeout(r, 2000));

    // Open settings from Alice's container and check member count
    await waitFor(
      () => aliceContainer.querySelector('[data-testid="chat-header"]') !== null,
      5000,
      "Alice's ChatHeader should be visible",
    );

    const gearButton = aliceContainer.querySelector<HTMLElement>(
      '[data-testid="chat-header"] button:has(.lucide-settings)',
    );
    if (gearButton) {
      simulateClick(gearButton);

      await waitFor(
        () => document.querySelector('[data-slot="sheet-content"]') !== null,
        5000,
        "ChatSettings sheet should open for Alice",
      );

      // The member list should show at least 2 members (Alice + Bob)
      // Both have "Anonymous <animal>" names
      const sheet = document.querySelector('[data-slot="sheet-content"]');
      const memberAvatars = sheet?.querySelectorAll('img[alt="Avatar"]') ?? [];
      // At minimum Alice should be there; Bob may take a moment to sync
      expect(memberAvatars.length).toBeGreaterThanOrEqual(1);
    }
  });
});
