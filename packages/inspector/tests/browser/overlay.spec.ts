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

test.describe("inspector overlay (embedded, shared runtime peer end-to-end)", () => {
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

  test("embedded inspector discovers and switches across auth-session runtimes", async ({
    page,
  }) => {
    const browserErrors: string[] = [];
    page.on("pageerror", (error) => browserErrors.push(error.stack ?? error.message));
    page.on("console", (message) => {
      if (message.type() === "error") browserErrors.push(message.text());
    });
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

    const hostResponse = await page.goto("/tests/browser/overlay-host.html");
    expect(hostResponse?.status()).toBe(200);

    // Host app stands up its real Jazz client and publishes the host handle.
    await expect(page.getByText("Host ready"))
      .toBeVisible({ timeout: 20_000 })
      .catch(async (error) => {
        const status = await page
          .locator("#host-status")
          .textContent({ timeout: 1_000 })
          .catch(() => null);
        const html = await page.content().catch(() => "unavailable");
        throw new Error(
          `${String(error)}\nHost status: ${status ?? "missing"}\nHTML: ${html.slice(0, 500)}\nBrowser errors: ${browserErrors.join("; ")}`,
        );
      });
    const registeredSessions = await page.evaluate(() => {
      const state = (globalThis as Record<PropertyKey, unknown>)[
        Symbol.for("jazz.browser-inspector-control-registry")
      ] as { factories?: Map<unknown, unknown> } | undefined;
      return state?.factories?.size ?? 0;
    });
    expect(registeredSessions).toBe(2);

    const inspector = page.frameLocator('iframe[title="jazz-inspector"]');

    // The host publishes persistent coordinates plus a control-port factory.
    // The separately bundled overlay never constructs a second SharedWorker.
    const overlayConfig = await page.evaluate(() => {
      const host = (
        window as unknown as {
          __jazzInspectorHost?: {
            getConnectionConfig(): {
              driver?: { type?: string };
            };
            openControlPort?: unknown;
          };
        }
      ).__jazzInspectorHost;
      return {
        driverType: host?.getConnectionConfig().driver?.type,
        hasControlPort: typeof host?.openControlPort === "function",
      };
    });
    expect(overlayConfig.driverType).toBe("persistent");
    expect(overlayConfig.hasControlPort).toBe(true);

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
    const runtimeSelect = inspector.getByLabel("Runtime context");
    await expect(runtimeSelect).toBeVisible({ timeout: 10_000 });
    const runtimeOptions = runtimeSelect.locator("option");
    await expect(runtimeOptions).toHaveCount(2);
    const primaryContext = runtimeOptions.filter({ hasNotText: "inspector-secondary-context" });
    const secondaryContext = runtimeOptions.filter({ hasText: "inspector-secondary-context" });
    const primaryContextValue = await primaryContext.getAttribute("value");
    const secondaryContextValue = await secondaryContext.getAttribute("value");
    expect(primaryContextValue).toBeTruthy();
    expect(secondaryContextValue).toBeTruthy();

    await runtimeSelect.selectOption(primaryContextValue!);
    await inspector.getByRole("link", { name: "View todos data" }).click();
    await expect(inspector.getByText("First seeded todo")).toBeVisible({ timeout: 30_000 });

    await runtimeSelect.selectOption(secondaryContextValue!);
    await expect(inspector.getByRole("link", { name: "Data Explorer" })).toBeVisible({
      timeout: 10_000,
    });

    // The host's `useAll(app.todos)` subscription is pushed to the Subscriptions tab.
    await inspector.getByRole("link", { name: "Subscriptions" }).click();
    await expect(inspector.getByRole("cell", { name: "todos", exact: true })).toBeVisible({
      timeout: 30_000,
    });
    expect(browserErrors).toEqual([]);
  });
});
