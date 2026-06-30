import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "@playwright/test";

const here = dirname(fileURLToPath(import.meta.url));
const packageRoot = join(here, "..", "..");
const distEmbedded = join(packageRoot, "dist-embedded");

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".wasm": "application/wasm",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

function extOf(path: string): string {
  const i = path.lastIndexOf(".");
  return i === -1 ? "" : path.slice(i);
}

test.describe("inspector overlay (embedded, own worker connection end-to-end)", () => {
  test.beforeAll(() => {
    // The embedded entry is a separate Vite build. Build it on demand so
    // `pnpm test:browser` works from a clean checkout; rebuild manually with
    // `pnpm --filter inspector run build:embedded` to refresh the assets.
    if (!existsSync(join(distEmbedded, "embedded.html"))) {
      execFileSync("pnpm", ["run", "build:embedded"], {
        cwd: packageRoot,
        stdio: "inherit",
      });
    }
  });

  test("embedded inspector opens its own worker connection from the published host handle", async ({
    page,
  }) => {
    // Serve dist-embedded/ to the iframe at the path it expects. The embedded
    // build uses base "./", so embedded.html requests `./assets/*`, which
    // resolve under /__jazz/embedded/assets/* — all matched here.
    await page.route("**/__jazz/embedded/**", async (route) => {
      const pathname = new URL(route.request().url()).pathname;
      const rel =
        pathname.replace(/^.*\/__jazz\/embedded\/?/, "").replace(/^\/+/, "") || "embedded.html";
      const filePath = join(distEmbedded, rel);
      try {
        const body = await readFile(filePath);
        await route.fulfill({
          contentType: MIME[extOf(filePath)] ?? "application/octet-stream",
          body,
        });
      } catch {
        await route.fulfill({ status: 404, body: "Not found" });
      }
    });

    await page.goto("/tests/browser/overlay-host.html");

    // Host app stands up its real Jazz client and publishes the host handle.
    await expect(page.getByText("Host ready")).toBeVisible({ timeout: 20_000 });

    const inspector = page.frameLocator('iframe[title="jazz-inspector"]');

    // The host publishes a resolved broker-worker URL, so the overlay's
    // persistent driver joins the host's SharedWorker/OPFS store (same url+name)
    // instead of spinning up an empty one. Without this it would default to its
    // own broker and never see the host's local data offline.
    const brokerWorkerUrl = await page.evaluate(
      () =>
        (
          window as unknown as {
            __jazzInspectorHost?: { getConnectionConfig(): { brokerWorkerUrl?: string } };
          }
        ).__jazzInspectorHost?.getConnectionConfig().brokerWorkerUrl,
    );
    expect(brokerWorkerUrl).toBeTruthy();

    // The overlay reads the handle, opens its connection joining that store, and
    // leaves the connecting state.
    await expect(inspector.getByText("Connecting…")).toBeHidden({ timeout: 30_000 });

    // It renders its real UI driven by the injected schema.
    await expect(inspector.getByRole("link", { name: "Data Explorer" })).toBeVisible({
      timeout: 30_000,
    });
    await expect(inspector.getByRole("link", { name: "View todos data" })).toBeVisible({
      timeout: 30_000,
    });

    // The host's `useAll(app.todos)` subscription is pushed to Live Query.
    await inspector.getByRole("link", { name: "Live Query" }).click();
    await expect(inspector.getByRole("cell", { name: "todos", exact: true })).toBeVisible({
      timeout: 30_000,
    });
  });
});
