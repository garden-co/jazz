import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const bundledSkillsDir = path.join(packageRoot, "skills");
const bundledJazzSkillDir = path.join(bundledSkillsDir, "jazz");

/** Install the Jazz guidance bundle in the portable project-local skill location. */
export function installJazzSkills(projectDir: string): void {
  if (!fs.existsSync(bundledJazzSkillDir)) {
    throw new Error(`Bundled Jazz skill is missing from ${bundledJazzSkillDir}.`);
  }

  const destination = path.join(projectDir, ".agents", "skills", "jazz");
  if (fs.existsSync(destination)) {
    throw new Error(
      `Refusing to overwrite existing Jazz agent skills at ${destination}. Remove or merge them manually.`,
    );
  }

  fs.cpSync(bundledJazzSkillDir, destination, {
    recursive: true,
    force: false,
    errorOnExist: true,
  });
}
