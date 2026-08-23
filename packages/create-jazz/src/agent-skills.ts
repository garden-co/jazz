import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const bundledSkillsDir = path.join(packageRoot, "skills");

/** Install the Jazz guidance bundle in the portable project-local skill location. */
export function installJazzSkills(projectDir: string): void {
  if (!fs.existsSync(bundledSkillsDir)) {
    throw new Error(`Bundled Jazz skills are missing from ${bundledSkillsDir}.`);
  }
  fs.cpSync(bundledSkillsDir, path.join(projectDir, ".agents", "skills"), {
    recursive: true,
    errorOnExist: true,
  });
}
