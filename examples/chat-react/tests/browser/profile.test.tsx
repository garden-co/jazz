/**
 * E2E browser tests for the profile feature.
 *
 * Tests profile name editing and avatar management.
 * Adapted from Jazz 1 Playwright profile.spec.ts.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { App } from "../../src/App.js";
import { TEST_PORT, APP_ID } from "./test-constants.js";

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

describe("Profile E2E", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  async function mountApp(
    config: {
      appId?: string;
      dbName?: string;
      serverUrl?: string;
    } = {},
  ): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);
    const r = createRoot(el);
    mounts.push({ root: r, container: el });

    const appId = config.appId ?? APP_ID;
    const serverUrl = config.serverUrl ?? `http://127.0.0.1:${TEST_PORT}`;

    await act(async () => {
      r.render(<App config={{ appId, serverUrl, ...config }} />);
    });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null || el.querySelector("article") !== null,
      10000,
      "App should render",
    );

    return el;
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
    window.location.hash = "";
    await new Promise((r) => setTimeout(r, 1000));
  });

  // -------------------------------------------------------------------------
  // 1. Update profile name
  // -------------------------------------------------------------------------

  it("can open profile and update name", async () => {
    const el = await mountApp({ dbName: uniqueDbName("profile") });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null,
      10000,
      "Editor should be visible",
    );

    // Wait for the NavBar Suspense to resolve
    await waitFor(
      () => el.querySelector('header [data-slot="dropdown-menu-trigger"]') !== null,
      5000,
      "NavBar menu button should appear",
    );

    // Open the menu
    const menuButton = el.querySelector<HTMLElement>('header [data-slot="dropdown-menu-trigger"]')!;
    await act(async () => simulateClick(menuButton));

    // The Profile item has data-slot="sheet-trigger" (SheetTrigger asChild
    // overrides the DropdownMenuItem's data-slot), so use role="menuitem".
    await waitFor(
      () => {
        const items = document.querySelectorAll('[role="menuitem"]');
        return [...items].some((i) => i.textContent?.includes("Profile"));
      },
      3000,
      "Profile menu item should appear",
    );

    const profileItem = [...document.querySelectorAll('[role="menuitem"]')].find((i) =>
      i.textContent?.includes("Profile"),
    ) as HTMLElement;
    await act(async () => simulateClick(profileItem));

    // Wait for the profile sheet to open
    await waitFor(
      () => document.querySelector('[data-slot="sheet-content"]') !== null,
      5000,
      "Profile sheet should open",
    );

    // Find the name input in the sheet and change it
    const nameInput = document.querySelector<HTMLInputElement>("#name");
    if (nameInput) {
      await act(async () => typeInto(nameInput, "Test User Name"));
    }

    // Close the sheet
    const closeButton = document
      .querySelector('[data-slot="sheet-content"] .lucide-x')
      ?.closest("button");
    if (closeButton) {
      await act(async () => simulateClick(closeButton as HTMLElement));
    }

    // Verify the name is updated in the navbar
    await waitFor(
      () => el.textContent?.includes("Test User Name") ?? false,
      5000,
      "Updated name should appear in the navbar",
    );
  });

  // -------------------------------------------------------------------------
  // 2. Upload and remove avatar
  // -------------------------------------------------------------------------

  it("can upload and remove avatar", async () => {
    const el = await mountApp({ dbName: uniqueDbName("avatar") });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null,
      10000,
      "Editor should be visible",
    );

    // Wait for the NavBar Suspense to resolve
    await waitFor(
      () => el.querySelector('header [data-slot="dropdown-menu-trigger"]') !== null,
      5000,
      "NavBar menu button should appear",
    );

    // Open the menu
    const menuButton = el.querySelector<HTMLElement>('header [data-slot="dropdown-menu-trigger"]')!;
    await act(async () => simulateClick(menuButton));

    // Use role="menuitem" because SheetTrigger overrides the data-slot
    await waitFor(
      () =>
        [...document.querySelectorAll('[role="menuitem"]')].some((i) =>
          i.textContent?.includes("Profile"),
        ),
      3000,
      "Profile menu item should appear",
    );

    const profileItem = [...document.querySelectorAll('[role="menuitem"]')].find((i) =>
      i.textContent?.includes("Profile"),
    ) as HTMLElement;
    await act(async () => simulateClick(profileItem));

    await waitFor(
      () => document.querySelector('[data-slot="sheet-content"]') !== null,
      5000,
      "Profile sheet should open",
    );

    // Wait for the ProfileContent Suspense to resolve
    await waitFor(
      () => document.querySelector<HTMLInputElement>('input[type="file"]#avatar') !== null,
      5000,
      "Avatar file input should appear",
    );

    const fileInput = document.querySelector<HTMLInputElement>('input[type="file"]#avatar')!;

    // Create a minimal PNG blob
    const pngBytes = Uint8Array.from(
      atob(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg==",
      ),
      (c) => c.charCodeAt(0),
    );
    const file = new File([pngBytes], "test-avatar.png", { type: "image/png" });

    // The file input renders in a Sheet portal, so React's event
    // delegation at the root container never receives the change event.
    // Use the exposed __handleAvatarChange instead, simulating evt.target.files.
    const dataTransfer = new DataTransfer();
    dataTransfer.items.add(file);
    fileInput!.files = dataTransfer.files;
    const handleChange = (fileInput as any).__handleAvatarChange as Function;
    expect(handleChange).toBeTruthy();
    // handleAvatarChange uses FileReader (callback-based), so we can't
    // simply await it. Call it and let the waitFor below detect the result.
    await act(async () => {
      handleChange({ target: fileInput! } as any);
    });

    // Wait for the uploaded avatar to appear (has object-cover class, vs the
    // default multiavatar which does not).
    await waitFor(
      () => {
        const sheetContent = document.querySelector('[data-slot="sheet-content"]');
        if (!sheetContent) return false;
        return sheetContent.querySelector("img.object-cover") !== null;
      },
      10000,
      "Avatar image should appear after upload",
    );

    // Remove the avatar
    await waitFor(
      () => [...document.querySelectorAll("button")].some((b) => b.textContent?.includes("Remove")),
      5000,
      "Remove button should appear after avatar upload",
    );
    const removeButton = [...document.querySelectorAll("button")].find((b) =>
      b.textContent?.includes("Remove"),
    ) as HTMLElement;
    await act(async () => simulateClick(removeButton));

    // Verify the avatar image is gone
    await waitFor(
      () => {
        const sheetContent = document.querySelector('[data-slot="sheet-content"]');
        if (!sheetContent) return false;
        return sheetContent.querySelector("img.object-cover") === null;
      },
      5000,
      "Avatar image should be removed",
    );
  });
});
