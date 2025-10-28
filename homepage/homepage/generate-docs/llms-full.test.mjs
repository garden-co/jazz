import assert from "node:assert";
import fs from "node:fs/promises";
import path from "node:path";
import { test } from "node:test";

test("Size test", async () => {
  const filePath = path.join(process.cwd(), "public", "llms-full.txt");
  const stats = await fs.stat(filePath);
  assert.ok(
    stats.size > 100 * 1024,
    "llms-full.txt should be larger than 100kb", // Somewhat arbitrary, but it's a good sanity check
  );
});

test("Content test", async () => {
  const filePath = path.join(process.cwd(), "public", "llms-full.txt");
  const content = await fs.readFile(filePath, "utf-8");
  assert.ok(
    content.includes(
      'Jazz authentication is based on cryptographic keys ("Account keys").',
    ),
    "Should contain authentication message", // From authentication, it's unlikely to change much
  );
});
