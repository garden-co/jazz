import { describe, it, expect, afterEach } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { resolveLocalDeps } from "./deps.js";
import { readSourceSnapshot, scaffold } from "./scaffold.js";

const root = path.resolve(import.meta.dirname, "../../..");
const commit = "a".repeat(40);
const directories: string[] = [];
const previous = process.env.JAZZ_STARTER_PATH;
afterEach(() => {
  if (previous === undefined) delete process.env.JAZZ_STARTER_PATH;
  else process.env.JAZZ_STARTER_PATH = previous;
  for (const directory of directories.splice(0))
    fs.rmSync(directory, { recursive: true, force: true });
});
function fixture(includeRn = false) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-preview-deps-"));
  directories.push(directory);
  const packages = ["create-jazz", "jazz-tools", ...(includeRn ? ["jazz-rn"] : [])].map((name) => {
    const destination = path.join(directory, name);
    fs.mkdirSync(destination);
    const source = path.join(
      root,
      name === "jazz-rn" ? "crates" : "packages",
      name,
      "package.json",
    );
    fs.copyFileSync(source, path.join(destination, "package.json"));
    return destination;
  });
  execFileSync(process.execPath, [
    path.join(root, "packages/create-jazz/scripts/write-preview-snapshot.mjs"),
    commit,
    ...packages,
  ]);
  return { directory, cli: packages[0]!, snapshot: readSourceSnapshot(packages[0]) };
}

describe("bundled preview dependency selection", () => {
  it("producer records precisely the packages published, including optional RN", () => {
    expect(Object.keys(fixture().snapshot.previewPackages!)).toEqual(["create-jazz", "jazz-tools"]);
    expect(fixture(true).snapshot.previewPackages!["jazz-rn"]).toBe(
      `https://pkg.pr.new/garden-co/jazz/jazz-rn@${commit}`,
    );
  });

  for (const value of [
    null,
    {},
    { "jazz-tools": "https://example.invalid/package" },
    {
      "create-jazz": `https://pkg.pr.new/garden-co/jazz/create-jazz@${commit}`,
      "jazz-tools": "https://pkg.pr.new/garden-co/jazz/jazz-tools@bbbbbbb",
    },
  ]) {
    it(`rejects invalid preview package authority ${JSON.stringify(value)}`, () => {
      const { cli } = fixture();
      const file = path.join(cli, "jazz-source-snapshot.json");
      const data = JSON.parse(fs.readFileSync(file, "utf8"));
      fs.writeFileSync(file, JSON.stringify({ ...data, packages: value }));
      expect(() => readSourceSnapshot(cli)).toThrow(/refusing to fall back/);
    });
  }

  it("scaffolds real starter dependencies from the preview without npm alpha fallback", async () => {
    const { directory, snapshot } = fixture();
    process.env.JAZZ_STARTER_PATH = path.join(root, "starters/ts-localfirst");
    const targetDir = path.join(directory, "app");
    await scaffold(
      { appName: "preview-app", targetDir, pm: null, git: false, starter: "ts-localfirst" },
      snapshot,
    );
    expect(
      JSON.parse(fs.readFileSync(path.join(targetDir, "package.json"), "utf8")).dependencies[
        "jazz-tools"
      ],
    ).toBe(`https://pkg.pr.new/garden-co/jazz/jazz-tools@${commit}`);
  });

  it("rejects an unpublished workspace dependency rather than resolving it to npm", async () => {
    const { snapshot } = fixture();
    await expect(
      resolveLocalDeps(
        { dependencies: { "jazz-rn": "workspace:^" } },
        root,
        undefined,
        snapshot.previewPackages,
      ),
    ).rejects.toThrow(/jazz-rn.*not published/);
  });

  it("preserves stdout-only package manager diagnostics", async () => {
    const { directory, snapshot } = fixture();
    process.env.JAZZ_STARTER_PATH = path.join(root, "starters/ts-localfirst");
    await expect(
      scaffold(
        {
          appName: "preview-app",
          targetDir: path.join(directory, "app"),
          pm: process.execPath,
          git: false,
          preInstall: async ({ dir }) => {
            fs.writeFileSync(
              path.join(dir, "install"),
              'console.log("ERR_PNPM_NO_MATCHING_VERSION planted"); process.exit(1);',
            );
          },
        },
        snapshot,
      ),
    ).rejects.toThrow(/ERR_PNPM_NO_MATCHING_VERSION planted/);
  });

  it("reports stdout-only and stderr package manager failures and preserves the scaffold", async () => {
    const { directory, snapshot } = fixture();
    process.env.JAZZ_STARTER_PATH = path.join(root, "starters/ts-localfirst");
    const targetDir = path.join(directory, "app");
    await expect(
      scaffold(
        {
          appName: "preview-app",
          targetDir,
          pm: process.execPath,
          git: false,
          preInstall: async ({ dir }) => {
            fs.writeFileSync(
              path.join(dir, "install"),
              'console.log("ERR_PNPM_NO_MATCHING_VERSION planted"); console.error("additional stderr detail"); process.exit(1);',
            );
          },
        },
        snapshot,
      ),
    ).rejects.toThrow(/ERR_PNPM_NO_MATCHING_VERSION planted[\s\S]*additional stderr detail/);
    expect(fs.existsSync(path.join(targetDir, "package.json"))).toBe(true);
  });
});
