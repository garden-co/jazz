import { execFileSync } from "node:child_process";

/** Device launch contains endpoint/control metadata only, never credentials. */
export function adb(args, { serial, exec = execFileSync } = {}) {
  return exec("adb", serial ? ["-s", serial, ...args] : args, { encoding: "utf8" });
}
