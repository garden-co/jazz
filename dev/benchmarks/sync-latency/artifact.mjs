// Benchmark-only build handoff; never a correctness-artifact authority.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

async function digest(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

export async function seal(archive, identity) {
  await writeFile(
    `${archive}.json`,
    JSON.stringify(
      {
        identity,
        sha256: await digest(archive),
      },
      null,
      2,
    ) + "\n",
  );
}

export async function verify(archive, identity) {
  const receipt = JSON.parse(await readFile(`${archive}.json`, "utf8"));
  assert.deepEqual(receipt.identity, identity, "benchmark build identity differs");
  assert.equal(receipt.sha256, await digest(archive), "benchmark archive changed");
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [action, archive] = process.argv.slice(2);
  assert(["seal", "verify"].includes(action) && archive, "usage: artifact.mjs seal|verify ARCHIVE");
  const git = (...args) => execFileSync("git", args, { encoding: "utf8" }).trim();
  const source = git("rev-parse", "HEAD");
  assert.equal(source, process.env.GITHUB_SHA, "checkout must match this workflow SHA");
  assert.equal(git("status", "--porcelain", "--untracked-files=no"), "", "dirty source");
  assert.match(process.env.GITHUB_RUN_ID ?? "", /^\d+$/, "workflow run ID required");
  const identity = {
    source,
    run: process.env.GITHUB_RUN_ID,
    node: process.version,
    platform: process.platform,
    arch: process.arch,
    lockfile: await digest("pnpm-lock.yaml"),
    nativeProfile: "release",
  };
  await (action === "seal" ? seal : verify)(archive, identity);
}
