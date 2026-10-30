import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { envEntries } from "./cloud-env.js";

/**
 * Generate a BETTER_AUTH_SECRET and write it to .env if not already set.
 * Idempotent — safe to call multiple times.
 */
export function writeBetterAuthSecret(dir: string): string | null {
  const envPath = join(dir, ".env");
  const existing = existsSync(envPath) ? readFileSync(envPath, "utf8") : "";
  if ([...envEntries(existing)].some(([key, value]) => key === "BETTER_AUTH_SECRET" && value)) {
    return null;
  }

  const secret = randomBytes(32).toString("base64url");
  const line = `BETTER_AUTH_SECRET=${secret}\n`;
  if (existing) {
    const prefix = existing.endsWith("\n") ? "" : "\n";
    appendFileSync(envPath, prefix + line, "utf8");
  } else {
    writeFileSync(envPath, line, "utf8");
  }
  return secret;
}
