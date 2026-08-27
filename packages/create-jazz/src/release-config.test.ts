import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { parse as parseYaml } from "yaml";

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

    const jazzFixedGroup = ["jazz-tools", "jazz-wasm", "jazz-napi", "jazz-rn", "create-jazz"];

    expect(config.fixed).toContainEqual(jazzFixedGroup);
    expect(createJazzPackage.version).toMatch(/^2\.0\.0-alpha\./);

    for (const packageName of jazzFixedGroup) {
      expect(preState.initialVersions?.[packageName]).toBe("2.0.0-alpha.6");
    }
  });

  it("establishes the exact source tag before publishing any package", () => {
    const workflow = parseYaml(
      fs.readFileSync(
        path.join(repoRoot, ".github", "workflows", "publish-jazz-tools-alpha.yml"),
        "utf8",
      ),
    ) as {
      jobs: {
        "publish-npm": {
          steps: Array<{ name?: string; run?: string }>;
        };
      };
    };
    const steps = workflow.jobs["publish-npm"].steps;
    const sourceTagIndex = steps.findIndex((step) => step.name === "Ensure release source tag");
    const firstPublishIndex = steps.findIndex((step) => step.name?.startsWith("Publish "));

    expect(sourceTagIndex).toBeGreaterThan(-1);
    expect(firstPublishIndex).toBeGreaterThan(-1);
    expect(sourceTagIndex).toBeLessThan(firstPublishIndex);
    expect(steps[sourceTagIndex]?.run).toContain("EXISTING_TAG_TARGET");
    expect(steps[sourceTagIndex]?.run).toContain("SOURCE_COMMIT");
    expect(steps[sourceTagIndex]?.run).toContain('git push origin "refs/tags/${TAG}"');
  });
});
