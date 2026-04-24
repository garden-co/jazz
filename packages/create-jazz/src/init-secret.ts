import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { join } from "node:path";

function hasKey(content: string, key: string): boolean {
  for (let line of content.split("\n")) {
    if (line.endsWith("\r")) line = line.slice(0, -1);
    if (!line || line.startsWith("#")) continue;
    const eq = line.indexOf("=");
    if (eq === -1) continue;
    if (line.slice(0, eq) === key && line.slice(eq + 1).length > 0) {
      return true;
    }
  }
  return false;
}

function writeSecret(dir: string, key: string, generate: () => string): string | null {
  const envPath = join(dir, ".env");
  const existing = existsSync(envPath) ? readFileSync(envPath, "utf8") : "";
  if (hasKey(existing, key)) return null;

  const secret = generate();
  const line = `${key}=${secret}\n`;
  if (existing) {
    const prefix = existing.endsWith("\n") ? "" : "\n";
    appendFileSync(envPath, prefix + line, "utf8");
  } else {
    writeFileSync(envPath, line, "utf8");
  }
  return secret;
}

/**
 * Generate a BETTER_AUTH_SECRET and write it to .env if not already set.
 * Idempotent — safe to call multiple times.
 */
export function writeBetterAuthSecret(dir: string): string | null {
  return writeSecret(dir, "BETTER_AUTH_SECRET", () => randomBytes(32).toString("base64url"));
}

/**
 * Generate a BACKEND_SECRET and write it to .env if not already set.
 * Idempotent — safe to call multiple times.
 */
export function writeBackendSecret(dir: string): string | null {
  return writeSecret(dir, "BACKEND_SECRET", () => randomBytes(32).toString("hex"));
}
