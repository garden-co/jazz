import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

// The pre-commit hook runs `oxlint --fix` on staged files (lefthook.yml). These
// spreads look redundant to a type-unaware rule but change behaviour if removed.
const fixture = `export function hex(digest: Uint8Array): string {
  return [...digest.slice(0, 16)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function notify(listeners: Set<() => void>): void {
  for (const listener of [...listeners]) listener();
}
`;

test("the pre-commit oxlint autofix keeps behaviour-carrying spreads", () => {
  const dir = mkdtempSync(path.join(tmpdir(), "oxlint-autofix-"));
  try {
    const file = path.join(dir, "fixture.ts");
    writeFileSync(file, fixture);
    execFileSync(path.join(root, "node_modules/.bin/oxlint"), ["--fix", file], {
      cwd: root,
      stdio: "pipe",
    });
    assert.equal(readFileSync(file, "utf8"), fixture);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
