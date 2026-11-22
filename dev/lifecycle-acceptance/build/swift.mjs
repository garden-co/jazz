// Swift on Linux: the jazz-swift package as the mirror tags it, with release-languages' libjazz_ffi
// through pkg-config.
import { cpSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { WORK, assert, filesUnder, run, sha256, timed } from "../lib/common.mjs";
import { candidate, cleanEnv, consumerProject, writeDriver } from "./consumer.mjs";
import { unpackFfi } from "./ffi.mjs";

export async function buildSwift() {
  const { version } = candidate();
  const ffi = await unpackFfi();
  const pc = [
    `prefix=${ffi.dir}`,
    "Name: jazz_ffi",
    "Description: Jazz core (release libjazz_ffi)",
    `Version: ${version}`,
    "Libs: -L${prefix} -ljazz_ffi",
    "Cflags: -I${prefix}/include",
    "",
  ].join("\n");
  writeFileSync(join(ffi.dir, "jazz_ffi.pc"), pc);

  const tree = join(WORK, "artifacts", "release-swift-mirror");
  const repo = join(WORK, "jazz-swift");
  rmSync(repo, { recursive: true, force: true });
  cpSync(tree, repo, { recursive: true });
  const git = (...args) =>
    run("git", ["-C", repo, "-c", "user.name=mirror", "-c", "user.email=mirror@localhost", ...args], { capture: true });
  git("init", "-q");
  git("add", "-A");
  git("commit", "-q", "-m", `jazz-swift ${version}`);
  git("tag", version);

  const dir = consumerProject("swift", { __JAZZ_SWIFT_URL__: `file://${repo}` });
  // The generated bindings belong to the target.
  cpSync(join(dir, "JazzApp.swift"), join(dir, "Sources", "driver", "JazzApp.swift"));
  const env = cleanEnv({ PKG_CONFIG_PATH: ffi.dir });
  const swift = ["--cache-path", join(WORK, "swiftpm-cache")];
  await timed("swift resolve", () => run("swift", ["package", ...swift, "resolve"], { cwd: dir, env }));

  const checkout = join(dir, ".build", "checkouts", "jazz-swift");
  const files = filesUnder(tree);
  const got = filesUnder(checkout, (name) => name === ".git");
  assert(got.join() === files.join(), "SwiftPM's checkout doesn't hold the mirror's files");
  for (const name of files)
    assert((await sha256(join(checkout, name))) === (await sha256(join(tree, name))), `SwiftPM resolved another ${name}`);
  await timed("swift build", () => run("swift", ["build", ...swift], { cwd: dir, env }));

  writeDriver("swift", {
    cmd: [join(dir, ".build", "debug", "driver")],
    env: { LD_LIBRARY_PATH: ffi.dir },
    loads: ffi.library,
    resolved: [
      { what: `jazz-swift ${version} Package.swift (mirror tree, ${files.length} files checked)`, sha256: await sha256(join(checkout, "Package.swift")) },
      { what: "libjazz_ffi-linux-x86_64.tar.gz", sha256: ffi.archive },
    ],
  });
}
