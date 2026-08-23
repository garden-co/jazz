import { execFileSync } from "node:child_process";
import * as path from "node:path";
import { describe, expect, it } from "vitest";

const packageRoot = path.resolve(import.meta.dirname, "..");
const bundledSkillFiles = [
  "skills/jazz/SKILL.md",
  "skills/jazz/references/application-data.md",
  "skills/jazz/references/authentication.md",
  "skills/jazz/references/schemas-and-permissions.md",
  "skills/jazz/references/testing.md",
];

describe("create-jazz package contents", () => {
  it("ships the bundled agent skills in npm pack output", () => {
    const output = execFileSync("npm", ["pack", "--json", "--dry-run", "--ignore-scripts"], {
      cwd: packageRoot,
      encoding: "utf8",
    });
    const packed = JSON.parse(output) as Array<{ files?: Array<{ path: string }> }>;
    const paths = new Set(packed[0]?.files?.map((file) => file.path));

    for (const file of bundledSkillFiles) {
      expect(paths).toContain(file);
    }
  }, 15_000);
});
