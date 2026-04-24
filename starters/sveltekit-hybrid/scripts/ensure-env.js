import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = join(__dirname, "..", ".env");

const required = [
  { key: "BETTER_AUTH_SECRET", value: () => randomBytes(32).toString("hex") },
  { key: "APP_ORIGIN", value: () => "http://localhost:5173" },
  { key: "BACKEND_SECRET", value: () => randomBytes(32).toString("hex") },
];

const existing = existsSync(envPath) ? readFileSync(envPath, "utf8") : "";
const missing = required.filter(({ key }) => !new RegExp(`^${key}=`, "m").test(existing));

if (missing.length === 0) {
  process.exit(0);
}

const appended = missing.map(({ key, value }) => `${key}=${value()}`).join("\n") + "\n";
writeFileSync(
  envPath,
  existing.length && !existing.endsWith("\n") ? existing + "\n" + appended : existing + appended,
);
console.log(`ensure-env: added ${missing.map((m) => m.key).join(", ")} to .env`);
