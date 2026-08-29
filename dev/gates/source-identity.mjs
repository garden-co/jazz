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

function excluded(pathname, pathspecs) {
  return pathspecs.some((pathspec) => {
    // This module owns the small pathspec vocabulary it accepts.  Keeping it
    // here lets ls-tree/ls-files share the same exclusions even though
    // `ls-tree` itself does not implement Git's exclude magic.
    const expression = pathspec
      .replace(/[|\\{}()[\]^$+?.]/g, "\\$&")
      .replaceAll("**", "\\u0000")
      .replaceAll("*", "[^/]*")
      .replaceAll("\\u0000", ".*");
    return new RegExp(`^${expression}$`).test(pathname);
  });
}

function filteredRecords(buffer, pathspecs, pathFromRecord = (record) => record) {
  const records = buffer.toString("utf8").split("\0").filter(Boolean);
  return Buffer.from(
    records
      .filter((record) => !excluded(pathFromRecord(record), pathspecs))
      .sort()
      .join("\0"),
  );
}

// This intentionally stores only opaque hashes. It distinguishes the checked
// out HEAD tree, index tree, unstaged patch, and every untracked path/content
// pair without placing source text or filenames in a receipt.
/**
 * An opaque identity for all source inputs a caller considers relevant.
 *
 * `excludePathspecs` is deliberately a Git pathspec list rather than a
 * post-hoc filename filter: tracked/index/unstaged and untracked sources must
 * be measured with the same boundary.  Build products are not source merely
 * because a developer has not happened to add the usual `.gitignore` yet.
 */
export function sourceIdentity(root, { excludePathspecs = [] } = {}) {
  const pathspec = [".", ...excludePathspecs.map((value) => `:(exclude)${value}`)];
  // Keep the historic object IDs for the common no-exclusions case.  A
  // filtered view cannot use `write-tree` (it necessarily includes every
  // staged path), so hash Git's canonical file records instead.
  const headTree =
    excludePathspecs.length === 0
      ? git(root, ["rev-parse", "HEAD^{tree}"], { encoding: "utf8" }).trim()
      : sha256(
          filteredRecords(
            git(root, ["ls-tree", "-r", "-z", "HEAD"]),
            excludePathspecs,
            (record) => record.slice(record.lastIndexOf("\t") + 1),
          ),
        );
  const indexTree =
    excludePathspecs.length === 0
      ? git(root, ["write-tree"], { encoding: "utf8" }).trim()
      : sha256(
          filteredRecords(
            git(root, ["ls-files", "-s", "-z"]),
            excludePathspecs,
            (record) => record.slice(record.lastIndexOf("\t") + 1),
          ),
        );
  const staged = sha256(git(root, ["diff", "--cached", "--no-ext-diff", "--binary", "--", ...pathspec]));
  const unstaged = sha256(
    git(root, ["diff", "--no-ext-diff", "--binary", "--", ...pathspec]),
  );
  const untracked = filteredRecords(
    git(root, ["ls-files", "--others", "--exclude-standard", "-z"]),
    excludePathspecs,
  )
    .toString("utf8")
    .split("\0")
    .filter(Boolean);
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
  const fields = { headTree, indexTree, staged, unstaged, untracked: untrackedHash.digest("hex") };
  return {
    ...fields,
    fingerprint: sha256(
      Object.entries(fields)
        .map(([key, value]) => `${key}\0${value}\0`)
        .join(""),
    ),
    dirty: staged !== sha256("") || unstaged !== sha256("") || untracked.length !== 0,
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
