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

test.describe("inspector overlay (embedded + relay end-to-end)", () => {
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

  test("embedded inspector connects to a host attachDevTools client via the overlay relay", async ({
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

    // Host app stands up its real Jazz client and attaches devtools.
    await expect(page.getByText("Host ready")).toBeVisible({ timeout: 20_000 });

    const inspector = page.frameLocator('iframe[title="jazz-inspector"]');

    // The embedded inspector starts in its waiting state and must leave it once
    // the runtime announces over the bridge.
    await expect(inspector.getByText("Waiting for runtime devtools connection...")).toBeHidden({
      timeout: 30_000,
    });

    // ...and render its real UI (InspectorLayout nav).
    await expect(inspector.getByRole("link", { name: "Data Explorer" })).toBeVisible({
      timeout: 30_000,
    });
  });
});
