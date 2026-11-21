// Comparison harness hooks for the InstantDB Wequencer (see ../../../../dev/app-comparisons/SPEC.md).
// The data lives in a hosted, temporary Instant app (no account): created on
// first use into .env, with instant.schema.ts and instant.perms.ts pushed on
// every seed. The Next server signs people in (email + password, minting
// Instant tokens) and serves the app.
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { serve, sh } from "../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4413;
const ORIGIN = `http://127.0.0.1:${PORT}`;
export const distDir = ".next/static";

/** Installs dependencies and creates the temporary Instant app on a fresh checkout. */
function setUp() {
  if (!existsSync(`${dir}/node_modules`)) sh(dir, "pnpm install --frozen-lockfile >&2");
  const file = `${dir}/.env`;
  if (!existsSync(file)) {
    const out = sh(dir, "pnpm exec instant-cli init-without-files --title wequencer-bench --temp");
    const { app } = JSON.parse(out.slice(out.indexOf("{")));
    writeFileSync(
      file,
      `NEXT_PUBLIC_INSTANT_APP_ID=${app.appId}\nINSTANT_APP_ADMIN_TOKEN=${app.adminToken}\n`,
    );
  }
  return Object.fromEntries(
    readFileSync(file, "utf8")
      .trim()
      .split("\n")
      .map((line) => line.split("=", 2)),
  );
}

export async function seed() {
  const env = setUp();
  sh(
    dir,
    `pnpm exec instant-cli push all --yes --app "$NEXT_PUBLIC_INSTANT_APP_ID" --token "$INSTANT_APP_ADMIN_TOKEN" >&2`,
    env,
  );
  const out = sh(dir, "pnpm exec tsx seed.ts", env);
  const ctx = JSON.parse(out.trim().split("\n").at(-1));
  writeFileSync(`${dir}/.bench-ctx.json`, JSON.stringify(ctx));
  return ctx;
}

/** The ctx of the last seed(), for --no-seed runs. */
export const lastSeed = () => JSON.parse(readFileSync(`${dir}/.bench-ctx.json`, "utf8"));

export async function start() {
  const env = setUp();
  sh(dir, "pnpm exec next build > /tmp/wequencer-instantdb-build.log", {
    ...env,
    NODE_ENV: "production",
  });
  const stop = await serve(dir, `pnpm exec next start -H 127.0.0.1 -p ${PORT}`, `${ORIGIN}/`, {
    ...env,
    NODE_ENV: "production",
  });
  return {
    url: ORIGIN,
    stop,
    where:
      "hosted (Instant Cloud temporary app, api.instantdb.com via CloudFront IAD / us-east); Next server local",
    rttUrl: "https://api.instantdb.com/",
  };
}

/** Signs `page` in as fixture user `key` through the app's sign-in form (untimed). */
export async function signIn(page, key, ctx) {
  const user = ctx.users[key];
  await page.goto(ORIGIN);
  await page.getByLabel("Email").fill(user.email);
  await page.getByLabel("Password").fill(user.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await page.waitForURL(`${ORIGIN}/dashboard`, { timeout: 120_000 });
  await page.getByRole("heading", { name: "Sessions" }).waitFor({ timeout: 120_000 });
  await coldStart(page, `${ORIGIN}/api/auth/sign-in`);
}

/**
 * Sign-in goes through the app's own form, which loads the app. The Jazz
 * hooks sign in through an API without loading a page, so their first timed
 * open starts cold. To match, this keeps only the Instant session (in
 * IndexedDB): it drops Instant's cached query results and the app's
 * localStorage, from a same-origin page that loads no app code, and clears the
 * context's HTTP cache.
 */
async function coldStart(page, plainUrl) {
  await page.goto(plainUrl);
  await page.evaluate(async () => {
    localStorage.clear();
    for (const { name } of await indexedDB.databases()) {
      if (!name?.startsWith("instant_")) continue;
      const db = await new Promise((resolve, reject) => {
        const request = indexedDB.open(name);
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      if (db.objectStoreNames.contains("querySubs"))
        await new Promise((resolve, reject) => {
          const tx = db.transaction("querySubs", "readwrite");
          tx.objectStore("querySubs").clear();
          tx.oncomplete = resolve;
          tx.onerror = () => reject(tx.error);
        });
      db.close();
    }
  });
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Network.clearBrowserCache");
  await cdp.detach();
  await page.goto("about:blank");
}

