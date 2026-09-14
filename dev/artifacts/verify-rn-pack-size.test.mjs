import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { verifyRnPackSize } from "./verify-rn-pack-size.mjs";
test("upload budget accepts recovery and rejects failed release and boundary", () => {
  assert.equal(verifyRnPackSize(193492106).base64Bytes, 257989476);
  assert.equal(verifyRnPackSize(193492106).estimatedRequestBytes, 258989476);
});
test("oversized and invalid tarballs fail closed", () => {
  for (const size of [209615782, 195000000, 194999999, 0, -1, NaN])
    assert.throws(() => verifyRnPackSize(size));
  assert.equal(verifyRnPackSize(194999997).base64Bytes, 259999996);
  assert.equal(verifyRnPackSize(1).base64OverheadBytes, 3);
});
test("assembly and release gate packs and publication consumes the verified tarball", () => {
  for (const file of ["build-jazz-packages.yml", "publish-jazz-tools-alpha.yml"])
    assert.match(
      readFileSync(new URL(`../../.github/workflows/${file}`, import.meta.url), "utf8"),
      /node dev\/artifacts\/rn-packages.mjs pack/,
    );
  const release = readFileSync(
    new URL("../../.github/workflows/publish-jazz-tools-alpha.yml", import.meta.url),
    "utf8",
  );
  assert.match(release, /node dev\/artifacts\/publish-rn-packages.mjs/);
});
