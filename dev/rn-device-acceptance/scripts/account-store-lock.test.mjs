import { test } from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";

test("native account preferences serialize processes and release on exception or crash", () => {
  const dir = mkdtempSync(join(tmpdir(), "jazz-account-lock-"));
  try {
    const binary = join(dir, "lock-test");
    execFileSync(
      "c++",
      [
        "-std=c++17",
        "-Wall",
        "-Wextra",
        "-Werror",
        "-I",
        fileURLToPath(new URL("../../../crates/jazz-rn/native", import.meta.url)),
        fileURLToPath(new URL("./fixtures/account-store-lock.cpp", import.meta.url)),
        "-o",
        binary,
      ],
      { stdio: "pipe", timeout: 30_000 },
    );
    execFileSync(binary, [dir], { stdio: "pipe", timeout: 10_000 });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
