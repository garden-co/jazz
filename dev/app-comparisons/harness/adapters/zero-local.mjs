// Shared helpers for the Zero ports' harness.mjs files: install the app on a
// fresh checkout, check the local Postgres (see <hero>/zero/NOTES.md for the
// one-time apt/initdb steps), and run zero-cache next to the app's server.
import { execSync } from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";
import { serve, sh } from "./vite-app.mjs";

export const PG_URL = (db) => `postgres://postgres:pass@127.0.0.1:5432/${db}`;

/** `pnpm install` on a fresh checkout (zero-sqlite3's native build is allowed in pnpm-workspace.yaml). */
export function ensureInstalled(dir) {
  if (!existsSync(`${dir}/node_modules/.bin/zero-cache`)) {
    execSync("pnpm install --frozen-lockfile", { cwd: dir, stdio: ["ignore", "inherit", "inherit"] });
  }
}

/** Starts the local Postgres cluster if needed and checks what Zero needs from it. */
export function ensurePostgres(dir) {
  try {
    execSync("pg_isready -h 127.0.0.1 -q");
  } catch {
    execSync("sudo service postgresql start", { stdio: "inherit" });
  }
  const check = `node -e "const pg=require('pg');const c=new pg.Client('${PG_URL("postgres")}');c.connect().then(()=>c.query('show wal_level')).then(r=>{console.log(r.rows[0].wal_level);return c.end()})"`;
  let wal;
  try {
    wal = execSync(check, { cwd: dir }).toString().trim();
  } catch {
    throw new Error("Can't reach postgres://postgres:pass@127.0.0.1:5432: see NOTES.md for the Postgres setup");
  }
  if (wal !== "logical") throw new Error(`Postgres wal_level is ${wal}, Zero needs logical: see NOTES.md`);
}

/** Runs zero-cache (production mode) against `db`, calling the app's endpoints at `origin`. */
export async function startZeroCache(dir, { port, db, origin }) {
  mkdirSync(`${dir}/.zero`, { recursive: true });
  const url = `http://127.0.0.1:${port}`;
  const stop = await serve(
    dir,
    "pnpm exec zero-cache",
    url,
    {
      NODE_ENV: "production",
      ZERO_UPSTREAM_DB: PG_URL(db),
      ZERO_REPLICA_FILE: ".zero/replica.db",
      ZERO_PORT: String(port),
      ZERO_ADMIN_PASSWORD: "bench",
      ZERO_QUERY_URL: `${origin}/api/zero/query`,
      ZERO_MUTATE_URL: `${origin}/api/zero/mutate`,
      ZERO_QUERY_FORWARD_COOKIES: "true",
      ZERO_MUTATE_FORWARD_COOKIES: "true",
      ZERO_LOG_LEVEL: "warn",
      ZERO_ENABLE_TELEMETRY: "false",
    },
    120_000,
  );
  return { url, stop };
}

export { serve, sh };
