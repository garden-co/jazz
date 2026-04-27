import { readFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "../../..");
const documentedCommandPrefix = "pnpm --filter skill-issues cli";

async function readRepoFile(path: string) {
  return readFile(join(repoRoot, path), "utf8");
}

async function runDocumentedCommand(args: string[]) {
  return new Promise<{ stdout: string; stderr: string; exitCode: number | null }>(
    (resolve, reject) => {
      const child = spawn("pnpm", ["--filter", "skill-issues", "cli", ...args], {
        cwd: repoRoot,
        stdio: ["ignore", "pipe", "pipe"],
      });
      const stdout: Buffer[] = [];
      const stderr: Buffer[] = [];

      child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
      child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
      child.on("error", reject);
      child.on("close", (exitCode) => {
        resolve({
          stdout: Buffer.concat(stdout).toString("utf8"),
          stderr: Buffer.concat(stderr).toString("utf8"),
          exitCode,
        });
      });
    },
  );
}

describe("issues skill cutover docs", () => {
  it("defines the repo-local issues skill frontmatter", async () => {
    const contents = await readRepoFile(".agents/skills/issues/SKILL.md");

    expect(contents).toMatch(/^---\nname: issues\n/m);
    expect(contents).toMatch(/^description: .*Jazz ideas and issues.*skill-issues CLI\.\n/m);
  });

  it("routes all skill operations through the skill-issues CLI", async () => {
    const contents = await readRepoFile(".agents/skills/issues/SKILL.md");

    expect(contents).toContain(`${documentedCommandPrefix} ...`);
    expect(contents).toContain("repo-local skill-issues CLI");
  });

  it("documents a command prefix that resolves without Jazz config", async () => {
    const contents = await readRepoFile(".agents/skills/issues/SKILL.md");

    expect(contents).toContain(`${documentedCommandPrefix} show <slug>`);

    const result = await runDocumentedCommand(["show"]);

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("Usage: issues show <slug>");
    expect(result.stderr).not.toContain('Command "issues" not found');
    expect(result.stderr).not.toContain("SKILL_ISSUES_APP_ID is required");
  });

  it("does not leave stale non-runnable command docs in the implementation plan", async () => {
    const contents = await readRepoFile("docs/superpowers/plans/2026-04-27-skill-issues.md");
    const staleCommandPrefix = ["pnpm --filter skill-issues", "exec issues"].join(" ");

    expect(contents).not.toContain(staleCommandPrefix);
    expect(contents).toContain(`${documentedCommandPrefix} auth init`);
    expect(contents).toContain(`${documentedCommandPrefix} export todo`);
  });

  it("forbids direct Markdown edits and Markdown fallback on verification failure", async () => {
    const contents = await readRepoFile(".agents/skills/issues/SKILL.md");

    expect(contents).toContain("Do not edit `todo/` Markdown files directly");
    expect(contents).toContain("Do not create, rename, or delete Markdown source files");
    expect(contents).toContain("Do not fall back to Markdown capture");
    expect(contents).toContain("unless the CLI verifies it");
  });

  it("documents the required issues CLI commands", async () => {
    const contents = await readRepoFile(".agents/skills/issues/SKILL.md");
    const commands = [
      `${documentedCommandPrefix} auth init`,
      `${documentedCommandPrefix} auth github`,
      `${documentedCommandPrefix} add issue <slug>`,
      `${documentedCommandPrefix} add idea <slug>`,
      `${documentedCommandPrefix} list`,
      `${documentedCommandPrefix} show <slug>`,
      `${documentedCommandPrefix} assign <slug> --me`,
      `${documentedCommandPrefix} status <slug> open`,
      `${documentedCommandPrefix} status <slug> in_progress`,
      `${documentedCommandPrefix} status <slug> done`,
      `${documentedCommandPrefix} export todo`,
    ];

    for (const command of commands) {
      expect(contents).toContain(command);
    }
  });

  it("defines OpenAI agent metadata for the issues skill", async () => {
    const contents = await readRepoFile(".agents/skills/issues/agents/openai.yaml");

    expect(contents).toMatch(/^name: Issues\n/m);
    expect(contents).toMatch(/^description: .*skill-issues CLI\.\n/m);
    expect(contents).toContain("instructions: ../SKILL.md");
  });

  it.each(["AGENTS.md", "CLAUDE.md"])(
    "%s points agents at the issues skill instead of Markdown quick capture",
    async (path) => {
      const contents = await readRepoFile(path);

      expect(contents).toContain("Use the `issues` skill");
      expect(contents).not.toContain("After every write to `todo/`");
      expect(contents).not.toContain("scripts/update-todo.sh");
      expect(contents).not.toContain("todo/ideas/{priority}/{idea-name}.md");
      expect(contents).not.toContain("todo/issues/{issue-name}.md");
      expect(contents).not.toContain("Template:");
    },
  );

  it(".gitignore excludes generated and exported issue state", async () => {
    const contents = await readRepoFile(".gitignore");

    expect(contents).toContain("todo/");
    expect(contents).toContain(".skill-issues/");
    expect(contents).toContain("examples/skill-issues/.skill-issues/");
  });
});
