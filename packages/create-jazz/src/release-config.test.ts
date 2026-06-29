import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

const repoRoot = path.resolve(import.meta.dirname, "..", "..", "..");

describe("release config", () => {
  it("keeps create-jazz on the lockstep Jazz alpha release train", () => {
    const config = JSON.parse(
      fs.readFileSync(path.join(repoRoot, ".changeset", "config.json"), "utf8"),
    ) as { fixed?: string[][] };
    const preState = JSON.parse(
      fs.readFileSync(path.join(repoRoot, ".changeset", "pre.json"), "utf8"),
    ) as { initialVersions?: Record<string, string> };
    const createJazzPackage = JSON.parse(
      fs.readFileSync(path.join(repoRoot, "packages", "create-jazz", "package.json"), "utf8"),
    ) as { version?: string };

    // The original alpha train entered prerelease together at alpha.6.
    const jazzAlphaTrain = ["jazz-tools", "jazz-wasm", "jazz-napi", "jazz-rn", "create-jazz"];
    // jazz-inspector joined the fixed group later (it was 0.0.0 at pre-enter, so
    // it has no alpha.6 baseline), but versions in lockstep from here on.
    const jazzFixedGroup = [...jazzAlphaTrain, "jazz-inspector"];

    expect(config.fixed).toContainEqual(jazzFixedGroup);
    expect(createJazzPackage.version).toMatch(/^2\.0\.0-alpha\./);

    for (const packageName of jazzAlphaTrain) {
      expect(preState.initialVersions?.[packageName]).toBe("2.0.0-alpha.6");
    }
  });
});
