import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { verifyElf } from "./linux-napi/verify-elf.mjs";

test(
  "ELF gate rejects newer and nonnumeric version needs independently of host libc",
  {
    skip: process.platform !== "linux" || !["x64", "arm64"].includes(process.arch),
  },
  () => {
    const dir = mkdtempSync(join(tmpdir(), "jazz-elf-gate-"));
    const platform = `linux-${process.arch}-gnu`;
    try {
      const source = join(dir, "fixture.c"),
        output = join(dir, "fixture.so");
      // Link against a synthetic versioned dependency, never the host's libc.
      // No fixture is executed. SONAME models the dynamic loader's dependency
      // contract, including version needs not tied to an actual glibc release
      // installed on the machine running this test.
      const dependency = (name, version, symbol) => {
        writeFileSync(source, `void ${symbol}(void) {}\n`);
        const script = join(dir, "versions.map");
        writeFileSync(script, `${version} { global: ${symbol}; };\n`);
        const path = join(dir, name);
        execFileSync("cc", [
          "-shared",
          "-fPIC",
          source,
          `-Wl,--version-script=${script}`,
          `-Wl,-soname,${name}`,
          "-o",
          path,
        ]);
        return path;
      };
      const build = (glibcVersion, extra) => {
        const libraries = [dependency("libc.so.6", glibcVersion, "libc_probe")];
        if (extra) libraries.push(dependency("libstdc++.so.6", extra, "cxx_probe"));
        writeFileSync(
          source,
          `extern void libc_probe(void); ${extra ? "extern void cxx_probe(void);" : ""}\nvoid fixture(void) { libc_probe(); ${extra ? "cxx_probe();" : ""} }\n`,
        );
        execFileSync("cc", ["-shared", "-fPIC", source, ...libraries, "-o", output]);
      };
      build("GLIBC_2.34");
      assert.equal(verifyElf(output, platform).versions.GLIBC, "2.34");
      assert.throws(
        () => verifyElf(output, process.arch === "x64" ? "linux-arm64-gnu" : "linux-x64-gnu"),
        /not an ELF64 shared object/,
      );
      for (const version of ["GLIBC_2.38", "GLIBC_ABI_DT_RELR"]) {
        build(version);
        if (version === "GLIBC_ABI_DT_RELR") {
          // Model a version-needed record with no versioned imported symbol,
          // as emitted for packed relative relocations. Removing only dynsym's
          // version associations leaves the dynamic dependency requirement intact.
          const table = join(dir, "symbol-versions");
          execFileSync("objcopy", ["--dump-section", `.gnu.version=${table}`, output]);
          const bytes = readFileSync(table);
          for (let i = 2; i < bytes.length; i += 2) bytes.writeUInt16LE(1, i);
          writeFileSync(table, bytes);
          execFileSync("objcopy", ["--update-section", `.gnu.version=${table}`, output]);
          assert.doesNotMatch(
            execFileSync("readelf", ["--wide", "--dyn-syms", output], { encoding: "utf8" }),
            /@GLIBC_ABI_DT_RELR/,
          );
        }
        assert.throws(
          () => verifyElf(output, platform),
          new RegExp(`${version}.*exceeding GLIBC_2.34`),
        );
      }
      for (const version of ["GLIBCXX_3.4.31", "CXXABI_1.3.14"]) {
        build("GLIBC_2.34", version);
        assert.throws(() => verifyElf(output, platform), new RegExp(`${version}.*exceeding`));
      }
      build("GLIBC_2.34", "GLIBCXX_3.4.29");
      assert.equal(verifyElf(output, platform).versions.GLIBCXX, "3.4.29");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  },
);
