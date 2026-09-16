import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { verifyElf } from "./linux-napi/verify-elf.mjs";

test(
  "ELF gate admits a baseline object and rejects a planted newer glibc dependency",
  { skip: process.platform !== "linux" || process.arch !== "x64" },
  () => {
    const dir = mkdtempSync(join(tmpdir(), "jazz-elf-gate-"));
    try {
      const source = join(dir, "fixture.c"),
        output = join(dir, "fixture.so");
      const build = (code) => {
        writeFileSync(source, code);
        execFileSync("cc", ["-shared", "-fPIC", source, "-o", output]);
      };
      build("#include <stdlib.h>\nvoid *fixture(unsigned long n) { return malloc(n); }\n");
      assert.equal(verifyElf(output, "linux-x64-gnu").versions.GLIBC, "2.2.5");
      assert.throws(() => verifyElf(output, "linux-arm64-gnu"), /not an ELF64 shared object/);
      // This regression is linked to the host's newer glibc, exactly the original
      // Ubuntu24 producer failure. No native fixture is ever loaded into Node.
      build(
        "extern long __isoc23_strtol(const char *, char **, int);\nlong fixture(const char *s) { return __isoc23_strtol(s, 0, 10); }\n",
      );
      assert.throws(() => verifyElf(output, "linux-x64-gnu"), /GLIBC_2.38.*exceeding GLIBC_2.34/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  },
);
