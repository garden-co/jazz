import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import type { BrowserContext, Page } from "playwright";
import type { RecoveryConfig, RecoveryRows } from "./indexeddb-pending-recovery-fixture.js";
import { evaluateHarnessOperation } from "./evaluate-harness.mjs";
import { stopJazzServerByUrl } from "./testing-server-node.js";

export async function recoverPendingIndexedDbWrites(
  currentContext: BrowserContext,
  currentPage: Page,
  config: RecoveryConfig,
): Promise<RecoveryRows> {
  const browserType = currentContext.browser()?.browserType();
  if (!browserType) throw new Error("Recovery regression requires a Playwright browser");
  const target = new URL("../../../../target/", import.meta.url);
  await mkdir(target, { recursive: true });
  const profile = await mkdtemp(fileURLToPath(new URL("indexeddb-pending-recovery-", target)));
  const url = new URL("/tests/browser/remote-db-harness.html", currentPage.url()).toString();
  let context: BrowserContext | undefined;
  let serverStopped = false;

  async function call<T>(page: Page, method: string): Promise<T> {
    // Keep the deadline outside the recovering worker and retain the operation
    // in its owning page rather than passing a long-lived promise through CDP.
    return evaluateHarnessOperation<T>(
      page,
      "/tests/browser/indexeddb-pending-recovery-fixture.ts",
      method,
      config,
      30_000,
    );
  }

  async function launch(): Promise<Page> {
    context = await browserType!.launchPersistentContext(profile, { headless: true });
    const page = await context.newPage();
    await page.goto(url, { waitUntil: "domcontentloaded" });
    await call(page, "open");
    return page;
  }

  try {
    const page = await launch();
    await call(page, "seed");
    assert.deepEqual(await call<RecoveryRows>(page, "read"), { marker: 0, versions: [] });
    await stopJazzServerByUrl(config.serverUrl);
    serverStopped = true;
    await call(page, "writePending");
    assert.deepEqual(await call<RecoveryRows>(page, "read"), {
      marker: 1,
      versions: Array.from({ length: 500 }, (_, index) => index),
    });
    console.info(
      "[pending-write recovery] local acknowledgement and exact same-session rows verified",
    );

    // No SDK shutdown: close the entire browser, preserving only its profile.
    // Unlike closing tabs and sleeping, this cannot retain a warm worker realm.
    await context!.close();
    context = undefined;
    const reopened = await launch();
    console.info("[pending-write recovery] browser reopened offline; reading recovered rows");
    return await call<RecoveryRows>(reopened, "read");
  } finally {
    // Never await db.shutdown here: admission may be blocking its worker.
    try {
      await context?.close();
    } finally {
      await rm(profile, { recursive: true, force: true });
      if (!serverStopped) await stopJazzServerByUrl(config.serverUrl);
    }
  }
}
