/**
 * E2E browser tests for upload functionality.
 *
 * Tests image and file uploads in the chat.
 * Adapted from Jazz 1 Playwright uploads.spec.ts.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { App } from "../../src/App.js";
import { Toaster } from "../../src/components/ui/sonner.js";
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

function simulateClick(el: HTMLElement) {
  el.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, cancelable: true }));
  el.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, cancelable: true }));
  el.click();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Upload E2E", () => {
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
      r.render(
        <>
          <App config={{ appId, serverUrl, ...config }} />
          <Toaster />
        </>,
      );
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
  // 1. Image upload
  // -------------------------------------------------------------------------

  it("uploads an image and displays it in chat", async () => {
    const el = await mountApp({ dbName: uniqueDbName("img-upload") });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null,
      10000,
      "Editor should be visible",
    );

    // Open the action menu and click "Image"
    const plusButton =
      el.querySelector<HTMLElement>("button:has(.lucide-plus)") ??
      [...el.querySelectorAll("button")].find((b) => b.querySelector(".lucide-plus"));
    expect(plusButton).toBeTruthy();
    await act(async () => simulateClick(plusButton as HTMLElement));

    await waitFor(
      () =>
        [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].some((i) =>
          i.textContent?.toLowerCase().includes("image"),
        ),
      3000,
      "Image menu item should appear",
    );

    const imageItem = [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].find((i) =>
      i.textContent?.toLowerCase().includes("image"),
    ) as HTMLElement;
    await act(async () => simulateClick(imageItem));

    // Wait for the upload dialog
    await waitFor(
      () => document.querySelector('input[type="file"]') !== null,
      5000,
      "File input should appear in upload dialog",
    );

    // Create a minimal PNG and set it on the file input
    const pngBytes = Uint8Array.from(
      atob(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg==",
      ),
      (c) => c.charCodeAt(0),
    );
    const file = new File([pngBytes], "test-image.png", { type: "image/png" });

    // The file input renders in a Radix Dialog portal, so React's event
    // delegation at the root container never receives the change event.
    // Use the exposed __handleFile instead.
    const fileInput = document.querySelector<HTMLInputElement>('input[type="file"]')!;
    const handleFile = (fileInput as any).__handleFile as (f: File) => Promise<void>;
    expect(handleFile).toBeTruthy();

    await act(async () => {
      await handleFile(file);
    });

    // Verify "upload successful" toast appears
    await waitFor(
      () => document.body.textContent?.toLowerCase().includes("upload successful") ?? false,
      10000,
      "Upload successful toast should appear",
    );

    // Verify the image appears in the chat
    await waitFor(
      () => el.querySelector("article img") !== null,
      5000,
      "Uploaded image should appear in chat",
    );
  });

  // -------------------------------------------------------------------------
  // 2. File upload
  // -------------------------------------------------------------------------

  it("uploads a file and displays it in chat", async () => {
    const el = await mountApp({ dbName: uniqueDbName("file-upload") });

    await waitFor(
      () => el.querySelector("#messageEditor") !== null,
      10000,
      "Editor should be visible",
    );

    // Open the action menu and click "File"
    const plusButton =
      el.querySelector<HTMLElement>("button:has(.lucide-plus)") ??
      [...el.querySelectorAll("button")].find((b) => b.querySelector(".lucide-plus"));
    expect(plusButton).toBeTruthy();
    await act(async () => simulateClick(plusButton as HTMLElement));

    await waitFor(
      () =>
        [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].some((i) =>
          i.textContent?.toLowerCase().includes("file"),
        ),
      3000,
      "File menu item should appear",
    );

    const fileItem = [...document.querySelectorAll('[data-slot="dropdown-menu-item"]')].find((i) =>
      i.textContent?.toLowerCase().includes("file"),
    ) as HTMLElement;
    await act(async () => simulateClick(fileItem));

    // Wait for the upload dialog
    await waitFor(
      () => document.querySelector('input[type="file"]') !== null,
      5000,
      "File input should appear in upload dialog",
    );

    // Create a test text file
    const testFileName = "test-upload-file.txt";
    const file = new File(["Hello upload test"], testFileName, { type: "text/plain" });

    const fileInput = document.querySelector<HTMLInputElement>('input[type="file"]')!;
    const handleFile = (fileInput as any).__handleFile as (f: File) => Promise<void>;
    expect(handleFile).toBeTruthy();
    await act(async () => {
      await handleFile(file);
    });

    // Verify "upload successful" toast appears
    await waitFor(
      () => document.body.textContent?.toLowerCase().includes("upload successful") ?? false,
      10000,
      "Upload successful toast should appear",
    );

    // Verify the file name appears in the chat
    await waitFor(
      () => el.textContent?.includes(testFileName) ?? false,
      5000,
      "Uploaded file name should appear in chat",
    );

    // Verify the download button is present
    await waitFor(
      () =>
        [...el.querySelectorAll("button, a")].some((b) =>
          b.textContent?.toLowerCase().includes("download"),
        ),
      5000,
      "Download button should appear for the uploaded file",
    );
  });
});
