import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { beforeEach, describe, expect, it } from "vitest";
import { writeBackendSecret, writeBetterAuthSecret } from "./init-secret.js";

let dir: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "init-secret-test-"));
});

function readEnv(): string {
  return readFileSync(join(dir, ".env"), "utf8");
}

describe("writeBetterAuthSecret", () => {
  it("writes BETTER_AUTH_SECRET when .env is absent", () => {
    const secret = writeBetterAuthSecret(dir);
    expect(secret).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(readEnv()).toContain(`BETTER_AUTH_SECRET=${secret}\n`);
  });

  it("is idempotent when the key is already present", () => {
    writeFileSync(join(dir, ".env"), "BETTER_AUTH_SECRET=preset\n");
    expect(writeBetterAuthSecret(dir)).toBeNull();
    expect(readEnv()).toBe("BETTER_AUTH_SECRET=preset\n");
  });

  it("appends to an existing .env without trailing newline", () => {
    writeFileSync(join(dir, ".env"), "OTHER=value");
    const secret = writeBetterAuthSecret(dir);
    expect(readEnv()).toBe(`OTHER=value\nBETTER_AUTH_SECRET=${secret}\n`);
  });
});

describe("writeBackendSecret", () => {
  it("writes BACKEND_SECRET when .env is absent", () => {
    const secret = writeBackendSecret(dir);
    expect(secret).toMatch(/^[0-9a-f]{64}$/);
    expect(readEnv()).toContain(`BACKEND_SECRET=${secret}\n`);
  });

  it("is idempotent when the key is already present", () => {
    writeFileSync(join(dir, ".env"), "BACKEND_SECRET=preset\n");
    expect(writeBackendSecret(dir)).toBeNull();
    expect(readEnv()).toBe("BACKEND_SECRET=preset\n");
  });

  it("appends alongside an existing BETTER_AUTH_SECRET entry", () => {
    writeFileSync(join(dir, ".env"), "BETTER_AUTH_SECRET=abc\n");
    const secret = writeBackendSecret(dir);
    expect(readEnv()).toBe(`BETTER_AUTH_SECRET=abc\nBACKEND_SECRET=${secret}\n`);
  });
});
