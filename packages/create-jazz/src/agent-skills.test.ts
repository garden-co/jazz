import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import { installJazzSkills } from "./agent-skills.js";

const temporaryDirectories: string[] = [];
const repoRoot = path.resolve(import.meta.dirname, "../../..");

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe("installJazzSkills", () => {
  it("installs a portable project-local skill with progressive references", () => {
    const projectDir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-skills-"));
    temporaryDirectories.push(projectDir);

    installJazzSkills(projectDir);

    const skillDir = path.join(projectDir, ".agents", "skills", "jazz");
    const skill = fs.readFileSync(path.join(skillDir, "SKILL.md"), "utf8");
    expect(skill).toContain("name: jazz");
    expect(skill).toContain("references/schemas-and-permissions.md");
    expect(
      fs.readFileSync(path.join(skillDir, "references", "application-data.md"), "utf8"),
    ).toContain("https://jazz.tools/docs/reading/queries");
  });

  it("preserves an existing project-local Jazz skill instead of overwriting it", () => {
    const projectDir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-skills-"));
    temporaryDirectories.push(projectDir);
    const skillPath = path.join(projectDir, ".agents", "skills", "jazz", "SKILL.md");

    fs.mkdirSync(path.dirname(skillPath), { recursive: true });
    fs.writeFileSync(skillPath, "custom project guidance\n");

    expect(() => installJazzSkills(projectDir)).toThrow(/Refusing to overwrite existing Jazz/);
    expect(fs.readFileSync(skillPath, "utf8")).toBe("custom project guidance\n");
  });

  it("links every bundled reference to a current canonical documentation page", () => {
    const referencesDir = path.join(
      repoRoot,
      "packages",
      "create-jazz",
      "skills",
      "jazz",
      "references",
    );
    const links = fs
      .readdirSync(referencesDir)
      .flatMap((file) =>
        Array.from(
          fs
            .readFileSync(path.join(referencesDir, file), "utf8")
            .matchAll(/https:\/\/jazz\.tools\/docs\/([a-z0-9/-]+)/g),
        ),
      );

    for (const [, slug] of links) {
      expect(fs.existsSync(path.join(repoRoot, "docs", "content", "docs", `${slug}.mdx`))).toBe(
        true,
      );
    }
  });
});
