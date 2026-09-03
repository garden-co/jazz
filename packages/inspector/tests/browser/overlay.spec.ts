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
              jwtToken?: string;
              runtimeSources?: {
                browserWorkerSession?: {
                  issuer?: string;
                  user_id?: string;
                  authMode?: string;
                };
              };
            };
            openControlPort?: unknown;
          };
        }
      ).__jazzInspectorHost;
      return {
        driverType: host?.getConnectionConfig().driver?.type,
        hasControlPort: typeof host?.openControlPort === "function",
        hasJwtToken: Boolean(host?.getConnectionConfig().jwtToken),
        browserWorkerSession: host?.getConnectionConfig().runtimeSources?.browserWorkerSession,
      };
    });
    expect(overlayConfig.driverType).toBe("persistent");
    expect(overlayConfig.hasControlPort).toBe(true);
    expect(overlayConfig.hasJwtToken).toBe(true);
    expect(overlayConfig.browserWorkerSession).toMatchObject({
      issuer: "urn:jazz:local-first",
      authMode: "local-first",
    });

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
    const primaryContext = runtimeOptions.filter({ hasText: "local-first" });
    const secondaryContext = runtimeOptions.filter({ hasText: "external" });
    const primaryContextValue = await primaryContext.getAttribute("value");
    const secondaryContextValue = await secondaryContext.getAttribute("value");
    expect(primaryContextValue).toBeTruthy();
    expect(secondaryContextValue).toBeTruthy();
    await runtimeSelect.selectOption(primaryContextValue!);
    await inspector.getByRole("link", { name: "View todos data" }).click();
    await expect(inspector.getByText("First seeded todo")).toBeVisible({ timeout: 30_000 });

    // The attached inspector peer must be admitted as the host's verified
    // local-first session for writes as well as reads. Leave the host's
    // readiness row intact while the inspector reconstructs its attachment.
    const writableRowTitle = "Second seeded todo";
    const writableRow = inspector.locator('[role="row"]').filter({
      has: inspector.getByRole("gridcell", { name: writableRowTitle, exact: true }),
    });
    const doneToggle = writableRow.getByRole("checkbox");
    const wasDone = await doneToggle.isChecked();
    await doneToggle.click();
    await expect(inspector.getByRole("status")).toContainText("Queued");
    await inspector.getByRole("button", { name: "Save changes" }).click();
    await expect(inspector.getByRole("button", { name: "Save changes" })).toHaveCount(0);

    // Reload only the inspector frame. The host remains live, while the
    // overlay must reconstruct its own peer and read the locally committed
    // write back through that fresh attachment.
    await page.locator('iframe[title="jazz-inspector"]').evaluate((iframe) => {
      iframe.contentWindow?.location.reload();
    });
    const reloadedInspector = page.frameLocator('iframe[title="jazz-inspector"]');
    await expect(reloadedInspector.getByRole("link", { name: "Data Explorer" })).toBeVisible({
      timeout: 30_000,
    });
    // A reload has no per-frame selected-context state. It must reconnect to
    // the host context, not choose an arbitrary sibling based on provider
    // registration order.
    await expect(reloadedInspector.getByLabel("Runtime context")).toHaveValue(primaryContextValue!);
    await reloadedInspector.getByRole("link", { name: "View todos data" }).click();
    const reloadedWritableRow = reloadedInspector.locator('[role="row"]').filter({
      has: reloadedInspector.getByRole("gridcell", { name: writableRowTitle, exact: true }),
    });
    await expect(reloadedWritableRow.getByRole("checkbox")).toHaveJSProperty("checked", !wasDone, {
      timeout: 30_000,
    });

    await reloadedInspector.getByLabel("Runtime context").selectOption(secondaryContextValue!);
    // The periodic context refresh only replaces a missing selection. It must
    // not undo an explicit later user choice in favour of the host default.
    await page.waitForTimeout(1_100);
    await expect(reloadedInspector.getByLabel("Runtime context")).toHaveValue(
      secondaryContextValue!,
    );
    await expect(reloadedInspector.getByRole("link", { name: "Data Explorer" })).toBeVisible({
      timeout: 10_000,
    });

    // This is an aggregated control session: switching changes the attached
    // worker peer, not merely a UI label. The secondary auth scope has no
    // primary rows, so the normal `useAll` call in the grid must use the
    // newly constructed Inspector-local source rather than a cached query
    // from the first context.
    await reloadedInspector.getByRole("link", { name: "Data Explorer" }).click();
    await expect(reloadedInspector.getByText("First seeded todo")).toBeHidden({ timeout: 30_000 });

    // The host's `useAll(app.todos)` subscription is pushed to the Subscriptions tab.
    await reloadedInspector.getByRole("link", { name: "Subscriptions" }).click();
    await expect(reloadedInspector.getByRole("cell", { name: "todos", exact: true })).toBeVisible({
      timeout: 30_000,
    });
    expect(browserErrors).toEqual([]);
  });
});
