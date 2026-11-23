import { execFileSync } from "node:child_process";
import {
  chmodSync,
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";

it.skipIf(process.platform === "win32").each([
  ["is missing", undefined],
  ["has no secret", "PORT=3000\n"],
  ["already has a secret", "PORT=3000\nBETTER_AUTH_SECRET=existing-test-value\n"],
])("keeps the development secret private when the env file %s", (_description, contents) => {
  const dir = mkdtempSync(join(tmpdir(), "band-chat-dev-secret-"));
  try {
    const scripts = join(dir, "scripts");
    mkdirSync(scripts);
    const script = join(scripts, "dev-secrets.mjs");
    copyFileSync(new URL("../scripts/dev-secrets.mjs", import.meta.url), script);
    const envFile = join(dir, ".env.development.local");
    if (contents !== undefined) {
      writeFileSync(envFile, contents);
      chmodSync(envFile, 0o644);
    }
    execFileSync(process.execPath, [script]);
    expect(statSync(envFile).mode & 0o777).toBe(0o600);
    const env = readFileSync(envFile, "utf8");
    expect(env).toMatch(/^BETTER_AUTH_SECRET=.+$/m);
    if (contents !== undefined) expect(env).toContain(contents);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
