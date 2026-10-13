// Run by `pnpm dev`. Creates per-checkout secrets for local runs in
// .env.development.local (git-ignored, loaded by `next dev` only), so no
// secret is ever checked in. Existing values are kept, so sessions and the
// Better Auth signing keys survive restarts.
import { randomBytes } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";

const FILE = new URL("../../.env.development.local", import.meta.url);
const NAMES = ["BACKEND_SECRET", "BETTER_AUTH_SECRET"];

const current = existsSync(FILE) ? readFileSync(FILE, "utf8") : "";
const missing = NAMES.filter((name) => !new RegExp(`^${name}=.+`, "m").test(current));
if (missing.length) {
  const lines = missing.map((name) => `${name}=${randomBytes(32).toString("base64url")}`);
  const prefix = current && !current.endsWith("\n") ? "\n" : "";
  writeFileSync(FILE, `${current}${prefix}${lines.join("\n")}\n`, { mode: 0o600 });
  console.log(`Generated local ${missing.join(" and ")} in .env.development.local`);
}
