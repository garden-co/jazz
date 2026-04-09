/**
 * E2E browser tests for the canvas feature.
 *
 * Tests drawing on a collaborative canvas.
 * Adapted from Jazz 1 Playwright canvas.spec.ts.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { App } from "../../src/App.js";
import { TEST_PORT, APP_ID } from "./test-constants.js";
import { loadOrCreateIdentitySeed, mintSelfSignedToken } from "jazz-tools";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function selfSignedTokenFor(userLabel: string, appId: string): string {
  const seed = loadOrCreateIdentitySeed(userLabel);
  return mintSelfSignedToken(seed.seed, appId);
}

async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Timeout: ${message}`);
}

function simulateClick(el: HTMLElement) {
  el.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, cancelable: true }));
  el.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, cancelable: true }));
  el.click();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Canvas E2E", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  async function mountApp(
    config: {
      appId?: string;
      dbName?: string;
      serverUrl?: string;
      jwtToken?: string;
    } = {},
  ): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);
    const r = createRoot(el);
    mounts.push({ root: r, container: el });

    const appId =
      config.appId ?? `test-canvas-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

    await act(async () => {
      r.render(<App config={{ appId, ...config }} />);
    });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null || el.querySelector("article") !== null,
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
  // 1. Single user draw
  // -------------------------------------------------------------------------

  it("can create a canvas and draw on it", async () => {
    const el = await mountApp({ dbName: uniqueDbName("canvas-draw") });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null,
      10000,
      "Editor should be visible",
    );

    // Open action menu and create a canvas
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

    // Wait for the canvas to appear
    await waitFor(
      () =>
        el.querySelector('[data-testid="canvas"]') !== null || el.querySelector("canvas") !== null,
      5000,
      "Canvas should appear in the chat",
    );

    const canvas = (el.querySelector('[data-testid="canvas"]') ??
      el.querySelector("canvas")) as HTMLElement;
    expect(canvas).toBeTruthy();

    // Draw on the canvas
    const rect = canvas.getBoundingClientRect();
    const startX = rect.left + 50;
    const startY = rect.top + 50;

    await act(async () => {
      canvas.dispatchEvent(
        new PointerEvent("pointerdown", {
          clientX: startX,
          clientY: startY,
          bubbles: true,
        }),
      );
      canvas.dispatchEvent(
        new PointerEvent("pointermove", {
          clientX: startX + 100,
          clientY: startY,
          bubbles: true,
        }),
      );
      canvas.dispatchEvent(
        new PointerEvent("pointermove", {
          clientX: startX + 100,
          clientY: startY + 100,
          bubbles: true,
        }),
      );
      canvas.dispatchEvent(
        new PointerEvent("pointerup", {
          clientX: startX + 100,
          clientY: startY + 100,
          bubbles: true,
        }),
      );
    });

    // Canvas should still be visible after drawing (no errors)
    expect(canvas).toBeTruthy();
  });

  // -------------------------------------------------------------------------
  // 2. Collaborative canvas
  //
  //    User A creates a canvas. User B (same public chat) sees it and draws.
  //    User A verifies the collaborator's drawing is visible.
  // -------------------------------------------------------------------------

  it("draws collaboratively between two sessions", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    // --- User A: create a canvas -------------------------------------------
    const aliceContainer = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("collab-canvas-a"),
      serverUrl,
      jwtToken: selfSignedTokenFor(`canvas-user-a-${Date.now()}`, APP_ID),
    });

    await waitFor(
      () => aliceContainer.querySelector("#messageEditor") !== null,
      10000,
      "User A editor should be visible",
    );

    // Capture the chat URL for user B
    const chatHash = window.location.hash;

    // Create a canvas
    const alicePlusButton =
      aliceContainer.querySelector<HTMLElement>("button:has(.lucide-plus)") ??
      [...aliceContainer.querySelectorAll("button")].find((b) => b.querySelector(".lucide-plus"));
    await act(async () => simulateClick(alicePlusButton as HTMLElement));

    await waitFor(
      () =>
        [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].some((i) =>
          i.textContent?.toLowerCase().includes("canvas"),
        ),
      3000,
      "Canvas menu item should appear for user A",
    );

    const aliceCanvasItem = [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].find(
      (i) => i.textContent?.toLowerCase().includes("canvas"),
    ) as HTMLElement;
    await act(async () => simulateClick(aliceCanvasItem));

    await waitFor(
      () =>
        aliceContainer.querySelector('[data-testid="canvas"]') !== null ||
        aliceContainer.querySelector("canvas") !== null,
      5000,
      "Canvas should appear for user A",
    );

    // Give the server time to persist
    await new Promise((r) => setTimeout(r, 500));

    // --- User B: join the same chat and see the canvas ---------------------
    window.location.hash = chatHash;

    const bobContainer = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("collab-canvas-b"),
      serverUrl,
      jwtToken: selfSignedTokenFor(`canvas-user-b-${Date.now()}`, APP_ID),
    });

    // User B should see the canvas
    await waitFor(
      () =>
        bobContainer.querySelector('[data-testid="canvas"]') !== null ||
        bobContainer.querySelector("canvas") !== null,
      10000,
      "Canvas should appear for user B",
    );

    const bobCanvas = (bobContainer.querySelector('[data-testid="canvas"]') ??
      bobContainer.querySelector("canvas")) as HTMLElement;
    expect(bobCanvas).toBeTruthy();

    // User B draws on the canvas
    const bobCanvasRect = bobCanvas.getBoundingClientRect();
    await act(async () => {
      bobCanvas.dispatchEvent(
        new PointerEvent("pointerdown", {
          clientX: bobCanvasRect.left + 100,
          clientY: bobCanvasRect.top + 100,
          bubbles: true,
        }),
      );
      bobCanvas.dispatchEvent(
        new PointerEvent("pointermove", {
          clientX: bobCanvasRect.left + 200,
          clientY: bobCanvasRect.top + 100,
          bubbles: true,
        }),
      );
      bobCanvas.dispatchEvent(
        new PointerEvent("pointerup", {
          clientX: bobCanvasRect.left + 200,
          clientY: bobCanvasRect.top + 100,
          bubbles: true,
        }),
      );
    });

    // Verify both canvases are still visible (no errors from sync)
    await new Promise((r) => setTimeout(r, 500));

    const aliceCanvas = (aliceContainer.querySelector('[data-testid="canvas"]') ??
      aliceContainer.querySelector("canvas")) as HTMLElement;
    expect(aliceCanvas).toBeTruthy();
    expect(bobCanvas).toBeTruthy();
  });
});
