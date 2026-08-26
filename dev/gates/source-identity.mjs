import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

function git(root, args, options = {}) {
  // CI container checkouts can be owned by the runner while commands execute
  // as the container user. Scope this exception to each read rather than
  // mutating the runner's global Git configuration. The identity still comes
  // from the exact checked-out repository and remains fail-closed on any Git
  // error.
  const result = spawnSync("git", ["-c", `safe.directory=${root}`, "-C", root, ...args], {
    encoding: null,
    ...options,
  });
  if (result.status !== 0) throw new Error(`git ${args.join(" ")} failed`);
  return result.stdout;
}
function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

// This intentionally stores only opaque hashes. It distinguishes the checked
// out HEAD tree, index tree, unstaged patch, and every untracked path/content
// pair without placing source text or filenames in a receipt.
export function sourceIdentity(root) {
  const headTree = git(root, ["rev-parse", "HEAD^{tree}"], { encoding: "utf8" }).trim();
  const indexTree = git(root, ["write-tree"], { encoding: "utf8" }).trim();
  const unstaged = sha256(git(root, ["diff", "--no-ext-diff", "--binary"]));
  const untracked = git(root, ["ls-files", "--others", "--exclude-standard", "-z"])
    .toString("utf8")
    .split("\0")
    .filter(Boolean)
    .sort();
  const untrackedHash = crypto.createHash("sha256");
  for (const relative of untracked) {
    const content = fs.readFileSync(path.join(root, relative));
    untrackedHash
      .update(Buffer.byteLength(relative).toString())
      .update("\0")
      .update(relative)
      .update("\0");
    untrackedHash.update(content.length.toString()).update("\0").update(content).update("\0");
  }
  const fields = { headTree, indexTree, unstaged, untracked: untrackedHash.digest("hex") };
  return {
    ...fields,
    fingerprint: sha256(
      Object.entries(fields)
        .map(([key, value]) => `${key}\0${value}\0`)
        .join(""),
    ),
    dirty: headTree !== indexTree || unstaged !== sha256("") || untracked.length !== 0,
  };
}

export function checkedOutCommit(root) {
  return git(root, ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
}

// Dependency setup is allowed to create ignored build products, but it must
// never alter the checked-out source used by a receipt.  A baseline captured
// immediately after checkout therefore remains the source attestation when
// the later measurement has the same tracked-source identity.
export function sameTrackedSource(left, right) {
  return (
    left?.commit === right?.commit &&
    left?.headTree === right?.headTree &&
    left?.indexTree === right?.indexTree &&
    left?.unstaged === right?.unstaged
  );
}
