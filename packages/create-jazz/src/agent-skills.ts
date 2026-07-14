import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

interface Command {
  executable: string;
  args: string[];
}

type CommandRunner = (
  executable: string,
  args: string[],
  options: { cwd: string; stdio: "pipe" },
) => unknown;

export function intentInstallCommand(packageManager: string): Command {
  switch (packageManager) {
    case "npm":
      return {
        executable: "npx",
        args: ["--yes", "@tanstack/intent@latest", "install", "--no-notices"],
      };
    case "pnpm":
      return {
        executable: "pnpm",
        args: ["dlx", "@tanstack/intent@latest", "install", "--no-notices"],
      };
    case "yarn":
      return {
        executable: "yarn",
        args: ["dlx", "@tanstack/intent@latest", "install", "--no-notices"],
      };
    case "bun":
      return {
        executable: "bunx",
        args: ["@tanstack/intent@latest", "install", "--no-notices"],
      };
    default:
      throw new Error(`Cannot set up Jazz coding skills with package manager "${packageManager}".`);
  }
}

export function setupAgentSkills(
  dir: string,
  packageManager: string,
  run: CommandRunner = execFileSync,
): void {
  const command = intentInstallCommand(packageManager);

  try {
    const packageJsonPath = join(dir, "package.json");
    const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf8")) as {
      intent?: Record<string, unknown>;
    };
    const configuredSkills = Array.isArray(packageJson.intent?.skills)
      ? packageJson.intent.skills.filter((skill): skill is string => typeof skill === "string")
      : [];
    packageJson.intent = {
      ...packageJson.intent,
      skills: [...new Set([...configuredSkills, "jazz-tools"])],
    };
    writeFileSync(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`);
    run(command.executable, command.args, { cwd: dir, stdio: "pipe" });
  } catch (error) {
    const stderr =
      error instanceof Error && "stderr" in error
        ? String((error as { stderr: Buffer | string }).stderr).trim()
        : "";
    throw new Error(
      `Jazz coding skill setup failed: ${stderr || (error instanceof Error ? error.message : String(error))}`,
    );
  }
}
