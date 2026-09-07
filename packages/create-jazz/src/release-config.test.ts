import fs from "node:fs";
import { execFileSync, spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
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
    ) as { version?: string; files?: string[] };

    const jazzFixedGroup = ["jazz-tools", "jazz-wasm", "jazz-napi", "jazz-rn", "create-jazz"];

    expect(config.fixed).toContainEqual(jazzFixedGroup);
    expect(createJazzPackage.version).toMatch(/^2\.0\.0-alpha\./);
    expect(createJazzPackage.files).toContain("jazz-source-snapshot.json");

    for (const packageName of jazzFixedGroup) {
      expect(preState.initialVersions?.[packageName]).toBe("2.0.0-alpha.6");
    }
  });

  it("binds pkg.pr.new create-jazz packages to the pull request head commit", () => {
    const workflow = parseYaml(
      fs.readFileSync(path.join(repoRoot, ".github", "workflows", "preview-build.yml"), "utf8"),
    ) as {
      jobs: {
        "publish-pkg-pr-new": { steps: Array<{ name?: string; run?: string; env?: unknown }> };
      };
    };
    const step = workflow.jobs["publish-pkg-pr-new"].steps.find(
      (candidate) => candidate.name === "Bind create-jazz preview to this immutable commit",
    );
    expect(step?.env).toEqual({ PREVIEW_COMMIT: "${{ github.event.pull_request.head.sha }}" });
    expect(step?.run).toContain("jazz-source-snapshot.json");
    expect(step?.run).toContain("schema:1");
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
      // Planted missing payload: the same packed-presence gate must fail.
      fs.rmSync(path.join(toolsDir, "dist/dev/inspector-overlay/embedded/embedded.html"));
      expect(runGate().status).not.toBe(0);
    } finally {
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  });

  it("packs and runs the production CLI fail-closed for invalid preview receipts", () => {
    const fixture = fs.mkdtempSync(path.join(tmpdir(), "create-jazz-preview-pack-"));
    const packageDir = path.join(fixture, "create-jazz");
    const packed = path.join(fixture, "packed");
    const extracted = path.join(fixture, "extracted");
    try {
      execFileSync("pnpm", ["--filter", "create-jazz", "build"], { cwd: repoRoot });
      fs.cpSync(path.join(repoRoot, "packages", "create-jazz"), packageDir, {
        recursive: true,
        filter: (source) => !source.includes(`${path.sep}node_modules`),
      });
      fs.copyFileSync(
        path.join(repoRoot, "pnpm-workspace.yaml"),
        path.join(fixture, "pnpm-workspace.yaml"),
      );
      const version = JSON.parse(
        fs.readFileSync(path.join(packageDir, "package.json"), "utf8"),
      ).version;
      fs.writeFileSync(
        path.join(packageDir, "jazz-source-snapshot.json"),
        `${JSON.stringify({ schema: 1, packageVersion: version, commit: "a".repeat(40) })}\n`,
      );
      fs.mkdirSync(packed);
      fs.mkdirSync(extracted);
      execFileSync("pnpm", ["pack", "--pack-destination", packed], { cwd: packageDir });
      const tarball = fs.readdirSync(packed).find((file) => file.endsWith(".tgz"));
      expect(tarball).toBeDefined();
      if (!tarball) throw new Error("create-jazz pack did not produce a tarball");
      execFileSync("tar", ["-xzf", path.join(packed, tarball!), "-C", extracted]);
      expect(
        JSON.parse(
          fs.readFileSync(path.join(extracted, "package/jazz-source-snapshot.json"), "utf8"),
        ),
      ).toEqual({ schema: 1, packageVersion: version, commit: "a".repeat(40) });
      const extractedPackage = path.join(extracted, "package");
      // Keep this receipt hermetic: only the packed CLI's behavior is under
      // test, so its dependencies are linked from the already-installed repo.
      fs.symlinkSync(
        path.join(repoRoot, "packages", "create-jazz", "node_modules"),
        path.join(extractedPackage, "node_modules"),
      );
      const env: NodeJS.ProcessEnv = {
        ...process.env,
        JAZZ_STARTER_PATH: path.join(repoRoot, "starters", "next-localfirst"),
      };
      delete env.npm_config_user_agent;
      for (const [name, receipt] of [
        ["malformed", "not json"],
        [
          "version-mismatch",
          JSON.stringify({
            schema: 1,
            packageVersion: "2.0.0-alpha.other",
            commit: "a".repeat(40),
          }),
        ],
      ]) {
        fs.writeFileSync(path.join(extractedPackage, "jazz-source-snapshot.json"), receipt);
        const appName = `invalid-${name}`;
        const result = spawnSync(
          process.execPath,
          [
            "bin/create-jazz.js",
            appName,
            "--starter",
            "next-localfirst",
            "--hosting",
            "selfhosted",
            "--no-git",
          ],
          {
            cwd: extractedPackage,
            env,
            encoding: "utf8",
            stdio: ["ignore", "pipe", "pipe"],
            timeout: 15_000,
          },
        );
        expect(result.status, `${result.stdout}\n${result.stderr}`).toBe(1);
        expect(result.stdout + result.stderr).toMatch(/invalid bundled preview source snapshot/i);
        expect(fs.existsSync(path.join(extractedPackage, appName))).toBe(false);
      }
    } finally {
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  }, 30_000);

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
