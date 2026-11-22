// libjazz_ffi-linux-x86_64 from release-languages, unpacked.
import { mkdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { WORK, run, sha256 } from "../lib/common.mjs";

export async function unpackFfi() {
  const archive = join(WORK, "artifacts", "release-languages", "libjazz_ffi-linux-x86_64.tar.gz");
  const dir = join(WORK, "ffi");
  rmSync(dir, { recursive: true, force: true });
  mkdirSync(dir, { recursive: true });
  run("tar", ["-xzf", archive, "-C", dir], { capture: true });
  return { dir, archive: await sha256(archive), library: await sha256(join(dir, "libjazz_ffi.so")) };
}
