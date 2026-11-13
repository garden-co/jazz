import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { verifyRnPackSize } from "./verify-rn-pack-size.mjs";
const receipt = JSON.parse(readFileSync(process.env.JAZZ_RN_VERIFIED_RECEIPT, "utf8"));
const names = ["jazz-rn-android", "jazz-rn-ios", "jazz-rn"];
if (receipt.length !== names.length) throw new Error("Incomplete RN publication receipt");
// Validate the complete set before the first registry mutation.
for (const [index, entry] of receipt.entries()) {
  if (entry.name !== names[index] || entry.version !== receipt[0].version)
    throw new Error("RN publication receipt is not a lock-stepped ordered set");
  const bytes = readFileSync(entry.tarball);
  verifyRnPackSize(bytes.length);
  if (createHash("sha256").update(bytes).digest("hex") !== entry.sha256)
    throw new Error(`Verified RN tarball changed: ${entry.name}`);
}
for (const entry of receipt) {
  const existing = spawnSync("npm", ["view", `${entry.name}@${entry.version}`, "version"], {
    stdio: "ignore",
  });
  if (existing.status === 0) {
    console.log(`${entry.name}@${entry.version} already published`);
    continue;
  }
  const publish = spawnSync(
    "npm",
    ["publish", entry.tarball, "--tag", "latest", "--access", "public"],
    { encoding: "utf8", stdio: ["ignore", "inherit", "pipe"] },
  );
  process.stderr.write(publish.stderr ?? "");
  if (publish.status === 0) continue;
  // npm accepts a large upload (the RN native packages are ~100 MB) before it
  // serves it: until then `npm view` finds nothing, and publishing again answers
  // E409 "Cannot publish over previously staged version". That version is
  // already ours on the registry, so treat it as published instead of stopping
  // the job before jazz-tools and create-jazz.
  if (/E409/.test(publish.stderr ?? "") && /previously staged version/.test(publish.stderr ?? "")) {
    console.log(
      `${entry.name}@${entry.version} is already staged on npm; treating it as published`,
    );
    continue;
  }
  throw new Error(`npm publish ${entry.name}@${entry.version} failed (exit ${publish.status})`);
}
