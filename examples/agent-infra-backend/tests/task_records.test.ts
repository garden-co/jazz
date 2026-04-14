import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createAgentDataStore } from "../src/index.js";
import { projectDoDesignerTasks, syncDoDesignerTasks } from "../src/task_records.js";

describe("syncDoDesignerTasks", () => {
  const tempDirs: string[] = [];

  afterEach(async () => {
    await Promise.all(
      tempDirs.splice(0, tempDirs.length).map((dir) =>
        rm(dir, { recursive: true, force: true }),
      ),
    );
  });

  it("imports Designer markdown task records into Jazz task rows", async () => {
    const tempDir = await mkdtemp(join(tmpdir(), "agent-task-sync-"));
    tempDirs.push(tempDir);

    const tasksRoot = join(tempDir, "tasks", "designer");
    await mkdir(tasksRoot, { recursive: true });

    await writeFile(
      join(tasksRoot, "d-001.md"),
      `# d-001 Get the entire Designer stack reviewable and mergeable

status: active
prio: P0
context: designer
project: prom/designer
updated: 2026-04-08

## Next

Fill in the smallest concrete next step.

## Context

Add the owning branch, workspace, issue, or plan anchors here.

## Notes

Captured through :do.

## Annotations

- 2026-04-08: Migrated from ~/do/now.md and promoted as the top active Designer task
`,
    );
    await writeFile(
      join(tasksRoot, "d-002.md"),
      `# d-002 Merge PR #3296 and clean up the rest of the open Designer PR stack

status: active
prio: P0
context: designer
project: prom/designer
branch: review/nikiv-designer-build123d-monaco-editor
pr: https://github.com/fl2024008/prometheus/pull/3296
updated: 2026-04-08

## Next

Fill in the smallest concrete next step.

## Context

Add the owning branch, workspace, issue, or plan anchors here.
`,
    );

    await writeFile(
      join(tempDir, "now.md"),
      `## Managed Tasks
- [d-001] Get the entire Designer stack reviewable and mergeable
- [d-002] Merge PR #3296 and clean up the rest of the open Designer PR stack
`,
    );
    await writeFile(join(tempDir, "next.md"), "## Managed Tasks\n");

    const store = createAgentDataStore({
      appId: "agent-task-sync-test",
      dataPath: join(tempDir, "agent-infra.db"),
    });

    try {
      const result = await syncDoDesignerTasks({
        store,
        tasksRoot,
        nowPath: join(tempDir, "now.md"),
        nextPath: join(tempDir, "next.md"),
      });

      expect(result.syncedCount).toBe(2);

      const tasks = await store.listTaskRecords({ context: "designer" });
      expect(tasks.map((task) => task.task_id)).toEqual(["d-001", "d-002"]);
      expect(tasks[0]?.annotations_json).toEqual([
        "- 2026-04-08: Migrated from ~/do/now.md and promoted as the top active Designer task",
      ]);
      expect(tasks[1]?.branch).toBe("review/nikiv-designer-build123d-monaco-editor");
      expect(tasks[1]?.pr).toBe("https://github.com/fl2024008/prometheus/pull/3296");
    } finally {
      await store.shutdown();
    }
  });

  it("projects Jazz-backed Designer tasks back into ~/do-style files", async () => {
    const tempDir = await mkdtemp(join(tmpdir(), "agent-task-project-"));
    tempDirs.push(tempDir);

    const tasksRoot = join(tempDir, "tasks", "designer");
    const nowPath = join(tempDir, "now.md");
    const nextPath = join(tempDir, "next.md");
    const designerPath = join(tempDir, "designer.md");
    await mkdir(tasksRoot, { recursive: true });

    await writeFile(nowPath, "## Managed Tasks\n\n## Legacy Backlog\nlegacy now\n");
    await writeFile(nextPath, "## Managed Tasks\n\n## Legacy Backlog\nlegacy next\n");
    await writeFile(designerPath, "## Managed Designer Tasks\n\n## Legacy Designer Scratch\nlegacy designer\n");

    const store = createAgentDataStore({
      appId: "agent-task-project-test",
      dataPath: join(tempDir, "agent-infra.db"),
    });

    try {
      await store.upsertTaskRecord({
        taskId: "d-001",
        context: "designer",
        title: "Keep the review stack publishable",
        status: "active",
        priority: "P0",
        placement: "now",
        focusRank: 1,
        project: "prom/designer",
        annotationsJson: ["- 2026-04-08: imported"],
      });
      await store.upsertTaskRecord({
        taskId: "d-002",
        context: "designer",
        title: "Move the next slice forward",
        status: "next",
        priority: "P1",
        placement: "next",
        focusRank: 1,
        project: "prom/designer",
      });

      const result = await projectDoDesignerTasks({
        store,
        tasksRoot,
        nowPath,
        nextPath,
        designerPath,
      });

      expect(result.projectedCount).toBe(2);
      expect(await readFile(join(tasksRoot, "d-001.md"), "utf8")).toContain("# d-001 Keep the review stack publishable");
      expect(await readFile(nowPath, "utf8")).toContain("- [d-001] Keep the review stack publishable");
      expect(await readFile(nowPath, "utf8")).toContain("legacy now");
      expect(await readFile(nextPath, "utf8")).toContain("- [d-002] Move the next slice forward");
      expect(await readFile(nextPath, "utf8")).toContain("legacy next");
      expect(await readFile(designerPath, "utf8")).toContain("- [d-001] P0 active Keep the review stack publishable");
      expect(await readFile(designerPath, "utf8")).toContain("legacy designer");
    } finally {
      await store.shutdown();
    }
  });
});
