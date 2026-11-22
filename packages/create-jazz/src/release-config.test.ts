import fs from "node:fs";
import { execFileSync, spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { parse as parseYaml } from "yaml";

const repoRoot = path.resolve(import.meta.dirname, "..", "..", "..");

describe("release config", () => {
  it("keeps create-jazz on the lockstep Jazz beta release train", () => {
    const config = JSON.parse(
      fs.readFileSync(path.join(repoRoot, ".changeset", "config.json"), "utf8"),
    ) as { fixed?: string[][] };
    const preState = JSON.parse(
      fs.readFileSync(path.join(repoRoot, ".changeset", "pre.json"), "utf8"),
    ) as { mode?: string; tag?: string; initialVersions?: Record<string, string> };
    const createJazzPackage = JSON.parse(
      fs.readFileSync(path.join(repoRoot, "packages", "create-jazz", "package.json"), "utf8"),
    ) as { version?: string };

    const originalJazzPackages = ["jazz-tools", "jazz-wasm", "jazz-napi", "jazz-rn", "create-jazz"];
    const jazzFixedGroup = [
      "jazz-tools",
      "jazz-wasm",
      "jazz-napi",
      "jazz-rn",
      "jazz-rn-ios",
      "jazz-rn-android",
      "create-jazz",
    ];

    expect(config.fixed).toContainEqual(jazzFixedGroup);
    expect(preState.mode).toBe("pre");
    expect(preState.tag).toBe("beta");
    // alpha until the first beta's Version Packages PR lands.
    expect(createJazzPackage.version).toMatch(/^2\.0\.0-(alpha|beta)\.[0-9]+$/);

    // The beta train was entered from 2.0.0-alpha.70.
    for (const packageName of originalJazzPackages) {
      expect(preState.initialVersions?.[packageName]).toBe("2.0.0-alpha.70");
    }
    // The payload packages joined later; they share the release version, not
    // the original packages' historical prerelease baseline.
    for (const platform of ["ios", "android"]) {
      const payload = JSON.parse(
        fs.readFileSync(
          path.join(repoRoot, "crates", "jazz-rn", "npm", platform, "package.json"),
          "utf8",
        ),
      ) as { name: string; version: string };
      expect(payload.name).toBe(`jazz-rn-${platform}`);
      expect(payload.version).toBe(createJazzPackage.version);
    }
  });

  it("stages and verifies embedded inspector assets before preview publication", () => {
    const workflow = parseYaml(
      fs.readFileSync(path.join(repoRoot, ".github/workflows/preview-build.yml"), "utf8"),
    );
    const steps = workflow.jobs["publish-pkg-pr-new"].steps as Array<{
      name?: string;
      run?: string;
    }>;
    const stage = steps.findIndex(
      (step) => step.name === "Stage inspector overlay assets into jazz-tools",
    );
    const verify = steps.findIndex(
      (step) => step.name === "Verify packed inspector overlay assets",
    );
    const publish = steps.findIndex((step) => step.name === "Publish to pkg.pr.new");
    expect(stage).toBeGreaterThan(-1);
    expect(verify).toBeGreaterThan(stage);
    expect(publish).toBeGreaterThan(verify);
    expect(steps[stage]?.run).toContain("pnpm --filter inspector run build:embedded");
    expect(steps[stage]?.run).toContain("pnpm --filter jazz-tools run stage:inspector-overlay");
    const fixture = fs.mkdtempSync(path.join(tmpdir(), "jazz-preview-inspector-"));
    try {
      const toolsDir = path.join(fixture, "packages/jazz-tools");
      const embedded = path.join(fixture, "packages/inspector/dist-embedded");
      fs.mkdirSync(path.join(toolsDir, "scripts"), { recursive: true });
      fs.mkdirSync(embedded, { recursive: true });
      fs.writeFileSync(path.join(embedded, "embedded.html"), "<html>fixture inspector</html>");
      // The page the backend serves at <basePath>/inspect/.
      fs.writeFileSync(
        path.join(embedded, "backend.html"),
        "<html>fixture backend inspector</html>",
      );
      fs.writeFileSync(
        path.join(fixture, "pnpm-workspace.yaml"),
        "packages:\n  - packages/jazz-tools\n",
      );
      const manifest = JSON.parse(
        fs.readFileSync(path.join(repoRoot, "packages/jazz-tools/package.json"), "utf8"),
      );
      fs.writeFileSync(
        path.join(toolsDir, "package.json"),
        JSON.stringify({ name: manifest.name, version: manifest.version, files: manifest.files }),
      );
      const stagingScript = path.join(toolsDir, "scripts/stage-inspector-overlay.mjs");
      fs.copyFileSync(
        path.join(repoRoot, "packages/jazz-tools/scripts/stage-inspector-overlay.mjs"),
        stagingScript,
      );
      execFileSync(process.execPath, [stagingScript]);
      const runGate = () =>
        spawnSync("bash", ["-e", "-c", steps[verify]!.run!], { cwd: fixture, encoding: "utf8" });
      const valid = runGate();
      expect(valid.status, valid.stderr).toBe(0);
      // Planted missing payload: the same packed-presence gate must fail,
      // for either page.
      for (const page of ["backend.html", "embedded.html"]) {
        const staged = path.join(toolsDir, "dist/dev/inspector-overlay/embedded", page);
        const kept = fs.readFileSync(staged);
        fs.rmSync(staged);
        expect(runGate().status, page).not.toBe(0);
        fs.writeFileSync(staged, kept);
      }
      // The module createJazz serves the Inspector from must carry the UI,
      // not the empty placeholder tsc emits.
      const uiModule = path.join(toolsDir, "dist/backend/inspector-ui-files.js");
      const generated = fs.readFileSync(uiModule);
      fs.writeFileSync(uiModule, "export const INSPECTOR_UI_FILES = null;\n");
      expect(runGate().status, "placeholder module").not.toBe(0);
      fs.writeFileSync(uiModule, generated);
      expect(runGate().status).toBe(0);
    } finally {
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  });

  it("tags the exact source commit before publishing the GitHub release", () => {
    // release-publish.yml publishes every registry; its finalize job pins
    // v<version> to the published commit, refusing a tag that points
    // elsewhere, before the draft goes public.
    const workflow = parseYaml(
      fs.readFileSync(path.join(repoRoot, ".github", "workflows", "release-publish.yml"), "utf8"),
    ) as {
      jobs: {
        finalize: {
          steps: Array<{ name?: string; run?: string }>;
        };
      };
    };
    const steps = workflow.jobs.finalize.steps;
    const tagIndex = steps.findIndex((step) => step.name?.startsWith("Tag v"));
    const publishIndex = steps.findIndex((step) => step.name === "Publish the draft as Latest");

    expect(tagIndex).toBeGreaterThan(-1);
    expect(publishIndex).toBeGreaterThan(-1);
    expect(tagIndex).toBeLessThan(publishIndex);
    expect(steps[tagIndex]?.run).toContain('[ "${existing}" = "${SHA}" ]');
    expect(steps[tagIndex]?.run).toContain('git push origin "${SHA}:refs/tags/${TAG}"');
  });
});
