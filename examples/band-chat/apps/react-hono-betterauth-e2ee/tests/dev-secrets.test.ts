import { execFileSync } from "node:child_process";
import {
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

it.skipIf(process.platform === "win32").each([true, false])(
  "writes a private development secret when the env file already exists: %s",
  (exists) => {
    const dir = mkdtempSync(join(tmpdir(), "band-chat-dev-secret-"));
    try {
      const scripts = join(dir, "scripts");
      mkdirSync(scripts);
      const script = join(scripts, "dev-secrets.mjs");
      copyFileSync(new URL("../scripts/dev-secrets.mjs", import.meta.url), script);
      const envFile = join(dir, ".env.development.local");
      if (exists) writeFileSync(envFile, "PORT=3000\n", { mode: 0o644 });
      execFileSync(process.execPath, [script]);
      expect(statSync(envFile).mode & 0o777).toBe(0o600);
      const env = readFileSync(envFile, "utf8");
      expect(env).toMatch(/^BETTER_AUTH_SECRET=.+$/m);
      if (exists) expect(env).toContain("PORT=3000");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  },
);
