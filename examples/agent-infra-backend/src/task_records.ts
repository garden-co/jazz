import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import path from "node:path";
import type { TaskRecord } from "../schema/app.js";
import type { AgentDataStore, UpsertTaskRecordInput } from "./store.js";

export interface SyncDoDesignerTasksInput {
  store: AgentDataStore;
  tasksRoot?: string;
  nowPath?: string;
  nextPath?: string;
  context?: string;
}

export interface ProjectDoDesignerTasksInput {
  store: AgentDataStore;
  tasksRoot?: string;
  nowPath?: string;
  nextPath?: string;
  designerPath?: string;
  context?: string;
}

interface FocusPlacement {
  placement: "now" | "next";
  focusRank: number;
}

const focusEntryPattern = /^- \[([a-z]-\d{3})\]\s+/;
const managedFocusHeader = "## Managed Tasks";
const legacyFocusHeader = "## Legacy Backlog";
const designerOverviewHeader = "## Managed Designer Tasks";
const designerOverviewLegacyTitle = "## Legacy Designer Scratch";

function expandHomePath(value: string): string {
  if (value === "~") {
    return homedir();
  }
  if (value.startsWith("~/")) {
    return path.join(homedir(), value.slice(2));
  }
  return value;
}

function cleanBlock(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const cleaned = value.trim();
  return cleaned === "" ? undefined : cleaned;
}

function parseOptionalCsv(value: string | undefined): string[] | undefined {
  if (!value) return undefined;
  const items = value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  return items.length > 0 ? items : undefined;
}

function inferPlacement(
  status: string,
  placement: FocusPlacement | undefined,
): "now" | "next" | "backlog" {
  if (placement) {
    return placement.placement;
  }
  switch (status.toLowerCase()) {
    case "active":
      return "now";
    case "next":
      return "next";
    default:
      return "backlog";
  }
}

function normalizeManagedPlacement(value: string): "now" | "next" | "backlog" {
  if (value === "now" || value === "next") {
    return value;
  }
  return "backlog";
}

function formatTaskDate(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => (typeof item === "string" ? item.trim() : ""))
    .filter(Boolean);
}

function optionalString(value: string | null | undefined): string | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  return trimmed === "" ? undefined : trimmed;
}

function renderTaskProjection(record: TaskRecord): string {
  const lines: string[] = [`# ${record.task_id} ${record.title}`, ""];
  const fields: Array<[string, string | undefined]> = [
    ["status", record.status],
    ["prio", record.priority],
    ["context", record.context],
    ["project", record.project],
    ["issue", optionalString(record.issue)],
    ["branch", optionalString(record.branch)],
    ["workspace", optionalString(record.workspace)],
    ["plan", optionalString(record.plan)],
    ["pr", optionalString(record.pr)],
    [
      "tags",
      asStringArray(record.tags_json).length > 0 ? asStringArray(record.tags_json).join(", ") : undefined,
    ],
    ["updated", formatTaskDate(record.updated_at)],
  ];

  for (const [key, value] of fields) {
    if (!value) {
      continue;
    }
    lines.push(`${key}: ${value}`);
  }

  lines.push("");

  const sections: Array<[string, string | undefined]> = [
    ["Next", optionalString(record.next_text)],
    ["Context", optionalString(record.context_text)],
    ["Notes", optionalString(record.notes_text)],
    ["Annotations", asStringArray(record.annotations_json).join("\n") || undefined],
  ];

  for (const [name, body] of sections) {
    lines.push(`## ${name}`, "");
    if (body) {
      lines.push(body);
    }
    lines.push("");
  }

  return `${lines.join("\n").replace(/\n+$/, "\n")}\n`;
}

async function readTextFile(pathValue: string): Promise<string> {
  try {
    return await readFile(pathValue, "utf8");
  } catch (error) {
    const message = error instanceof Error ? error : null;
    if ((message as NodeJS.ErrnoException | null)?.code === "ENOENT") {
      return "";
    }
    throw error;
  }
}

async function readFocusProjection(
  filePath: string,
): Promise<{ managed: string; legacy: string; hasManagedSection: boolean }> {
  const text = (await readTextFile(filePath)).replace(/\r\n/g, "\n");
  const managedIndex = text.indexOf(managedFocusHeader);
  const legacyIndex = text.indexOf(legacyFocusHeader);
  if (managedIndex < 0 || legacyIndex < 0 || legacyIndex < managedIndex) {
    return {
      managed: "",
      legacy: text.trim(),
      hasManagedSection: false,
    };
  }
  return {
    managed: text.slice(managedIndex + managedFocusHeader.length, legacyIndex).trim(),
    legacy: text.slice(legacyIndex + legacyFocusHeader.length).trim(),
    hasManagedSection: true,
  };
}

async function writeFocusProjection(
  filePath: string,
  entries: TaskRecord[],
  placement: "now" | "next",
  legacy: string,
): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
  const lines = [managedFocusHeader];
  for (const record of entries) {
    if (record.placement !== placement) {
      continue;
    }
    lines.push(`- [${record.task_id}] ${record.title}`);
  }
  lines.push("", legacyFocusHeader);
  if (legacy.trim() !== "") {
    lines.push(legacy.trim());
  }
  await writeFile(filePath, `${lines.join("\n").replace(/\n+$/, "\n")}\n`, "utf8");
}

function parseFocusEntryIds(body: string): string[] {
  if (body.trim() === "") {
    return [];
  }
  const ids: string[] = [];
  for (const rawLine of body.split(/\r?\n/)) {
    const match = focusEntryPattern.exec(rawLine.trim());
    if (!match) {
      continue;
    }
    ids.push(match[1]);
  }
  return ids;
}

function taskRecordToUpsertInput(
  record: TaskRecord,
  placement: "now" | "next" | "backlog",
  focusRank: number | undefined,
): UpsertTaskRecordInput {
  return {
    taskId: record.task_id,
    context: record.context,
    title: record.title,
    status: record.status,
    priority: record.priority,
    placement,
    focusRank,
    project: record.project,
    issue: record.issue ?? undefined,
    branch: record.branch ?? undefined,
    workspace: record.workspace ?? undefined,
    plan: record.plan ?? undefined,
    pr: record.pr ?? undefined,
    tagsJson: record.tags_json ?? undefined,
    nextText: record.next_text ?? undefined,
    contextText: record.context_text ?? undefined,
    notesText: record.notes_text ?? undefined,
    annotationsJson: record.annotations_json ?? undefined,
    sourceKind: record.source_kind ?? undefined,
    sourcePath: record.source_path ?? undefined,
    metadataJson: record.metadata_json ?? undefined,
    createdAt: record.created_at,
    updatedAt: new Date(),
  };
}

async function reconcileFocusProjectionWithJazz(
  store: AgentDataStore,
  records: TaskRecord[],
  nowProjection: { managed: string; legacy: string; hasManagedSection: boolean },
  nextProjection: { managed: string; legacy: string; hasManagedSection: boolean },
  context: string,
): Promise<TaskRecord[]> {
  if (!nowProjection.hasManagedSection && !nextProjection.hasManagedSection) {
    return records;
  }

  const desired = new Map<string, { placement: "now" | "next"; focusRank: number }>();
  for (const [index, taskId] of parseFocusEntryIds(nowProjection.managed).entries()) {
    desired.set(taskId.toLowerCase(), {
      placement: "now",
      focusRank: index + 1,
    });
  }
  for (const [index, taskId] of parseFocusEntryIds(nextProjection.managed).entries()) {
    const key = taskId.toLowerCase();
    if (desired.has(key)) {
      continue;
    }
    desired.set(key, {
      placement: "next",
      focusRank: index + 1,
    });
  }
  if (desired.size === 0) {
    return records;
  }

  let changed = false;
  for (const record of records) {
    const desiredState = desired.get(record.task_id.toLowerCase());
    const nextPlacement =
      desiredState?.placement ??
      (record.placement === "now" || record.placement === "next"
        ? "backlog"
        : normalizeManagedPlacement(record.placement));
    const nextFocusRank = desiredState?.focusRank ?? (nextPlacement === "backlog" ? undefined : record.focus_rank ?? undefined);
    const currentFocusRank = record.focus_rank ?? undefined;

    if (record.placement === nextPlacement && currentFocusRank === nextFocusRank) {
      continue;
    }

    await store.upsertTaskRecord(taskRecordToUpsertInput(record, nextPlacement, nextFocusRank));
    changed = true;
  }

  if (!changed) {
    return records;
  }

  return store.listTaskRecords({
    context,
    limit: 200,
  });
}

async function readManagedLegacyBody(
  filePath: string,
  managedHeader: string,
  legacyHeader: string,
): Promise<string> {
  const text = (await readTextFile(filePath)).replace(/\r\n/g, "\n");
  const managedIndex = text.indexOf(managedHeader);
  const legacyIndex = text.indexOf(legacyHeader);
  if (managedIndex < 0 || legacyIndex < 0 || legacyIndex < managedIndex) {
    return text.trim();
  }
  return text.slice(legacyIndex + legacyHeader.length).trim();
}

async function writeManagedLegacyBody(
  filePath: string,
  managedHeader: string,
  managedLines: string[],
  legacyHeader: string,
  legacyBody: string,
): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
  const lines = [managedHeader, ...managedLines, "", legacyHeader];
  if (legacyBody.trim() !== "") {
    lines.push(legacyBody.trim());
  }
  await writeFile(filePath, `${lines.join("\n").replace(/\n+$/, "\n")}\n`, "utf8");
}

async function readFocusPlacements(
  focusPath: string,
  placement: "now" | "next",
): Promise<Map<string, FocusPlacement>> {
  const placements = new Map<string, FocusPlacement>();
  const text = await readFile(focusPath, "utf8");
  let focusRank = 0;

  for (const rawLine of text.split(/\r?\n/)) {
    const match = focusEntryPattern.exec(rawLine.trim());
    if (!match) {
      continue;
    }
    focusRank += 1;
    placements.set(match[1], { placement, focusRank });
  }

  return placements;
}

function parseTaskFile(
  absolutePath: string,
  text: string,
  placementMap: Map<string, FocusPlacement>,
  context: string,
): UpsertTaskRecordInput {
  const lines = text.split(/\r?\n/);
  const heading = lines[0]?.trim() ?? "";
  const headingMatch = /^#\s+([a-z]-\d{3})\s+(.+)$/.exec(heading);
  if (!headingMatch) {
    throw new Error(`invalid task heading in ${absolutePath}`);
  }

  const [, taskId, title] = headingMatch;
  const fields = new Map<string, string>();
  const sections = new Map<string, string[]>();
  let currentSection: string | null = null;

  for (const rawLine of lines.slice(1)) {
    const line = rawLine.trimEnd();
    if (line.startsWith("## ")) {
      currentSection = line.slice(3).trim().toLowerCase();
      sections.set(currentSection, []);
      continue;
    }
    if (currentSection) {
      sections.get(currentSection)?.push(rawLine);
      continue;
    }
    const fieldMatch = /^([a-z_]+):\s*(.*)$/.exec(line.trim());
    if (fieldMatch) {
      fields.set(fieldMatch[1], fieldMatch[2]);
    }
  }

  const placement = placementMap.get(taskId);
  const annotations = cleanBlock(sections.get("annotations")?.join("\n"))
    ?.split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean);

  const updated = fields.get("updated");
  const timestamp = updated ? new Date(updated) : new Date();

  return {
    taskId,
    context: fields.get("context") ?? context,
    title: title.trim(),
    status: fields.get("status") ?? "backlog",
    priority: fields.get("prio") ?? fields.get("priority") ?? "P2",
    placement: inferPlacement(fields.get("status") ?? "backlog", placement),
    focusRank: placement?.focusRank,
    project: fields.get("project") ?? "",
    issue: fields.get("issue") || undefined,
    branch: fields.get("branch") || undefined,
    workspace: fields.get("workspace") || undefined,
    plan: fields.get("plan") || undefined,
    pr: fields.get("pr") || undefined,
    tagsJson: parseOptionalCsv(fields.get("tags")),
    nextText: cleanBlock(sections.get("next")?.join("\n")),
    contextText: cleanBlock(sections.get("context")?.join("\n")),
    notesText: cleanBlock(sections.get("notes")?.join("\n")),
    annotationsJson: annotations && annotations.length > 0 ? annotations : undefined,
    sourceKind: "do_markdown",
    sourcePath: absolutePath,
    metadataJson: {
      imported_from: "do",
      imported_context: context,
    },
    createdAt: timestamp,
    updatedAt: timestamp,
  };
}

export async function syncDoDesignerTasks({
  store,
  tasksRoot = "~/do/tasks/designer",
  nowPath = "~/do/now.md",
  nextPath = "~/do/next.md",
  context = "designer",
}: SyncDoDesignerTasksInput) {
  const tasksDir = expandHomePath(tasksRoot);
  const [nowPlacements, nextPlacements] = await Promise.all([
    readFocusPlacements(expandHomePath(nowPath), "now"),
    readFocusPlacements(expandHomePath(nextPath), "next"),
  ]);

  const placementMap = new Map<string, FocusPlacement>();
  for (const [taskId, placement] of nextPlacements) {
    placementMap.set(taskId, placement);
  }
  for (const [taskId, placement] of nowPlacements) {
    placementMap.set(taskId, placement);
  }

  const entries = await readdir(tasksDir, { withFileTypes: true });
  const taskIds: string[] = [];

  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".md")) {
      continue;
    }
    const absolutePath = path.join(tasksDir, entry.name);
    const text = await readFile(absolutePath, "utf8");
    const payload = parseTaskFile(absolutePath, text, placementMap, context);
    await store.upsertTaskRecord(payload);
    taskIds.push(payload.taskId);
  }

  const records = await store.listTaskRecords({
    context,
    limit: taskIds.length || 1,
  });

  return {
    syncedCount: taskIds.length,
    taskIds: taskIds.sort(),
    records,
  };
}

export async function projectDoDesignerTasks({
  store,
  tasksRoot = "~/do/tasks/designer",
  nowPath = "~/do/now.md",
  nextPath = "~/do/next.md",
  designerPath = "~/do/designer.md",
  context = "designer",
}: ProjectDoDesignerTasksInput) {
  const tasksDir = expandHomePath(tasksRoot);
  const nowFile = expandHomePath(nowPath);
  const nextFile = expandHomePath(nextPath);
  const designerFile = expandHomePath(designerPath);
  const [nowProjection, nextProjection, designerLegacy] = await Promise.all([
    readFocusProjection(nowFile),
    readFocusProjection(nextFile),
    readManagedLegacyBody(designerFile, designerOverviewHeader, designerOverviewLegacyTitle),
  ]);

  let records = await store.listTaskRecords({
    context,
    limit: 200,
  });
  records = await reconcileFocusProjectionWithJazz(
    store,
    records,
    nowProjection,
    nextProjection,
    context,
  );

  await mkdir(tasksDir, { recursive: true });

  const activeTaskIds = new Set<string>();
  for (const record of records) {
    activeTaskIds.add(record.task_id.toLowerCase());
    await writeFile(path.join(tasksDir, `${record.task_id}.md`), renderTaskProjection(record), "utf8");
  }

  const taskEntries = await readdir(tasksDir, { withFileTypes: true });
  for (const entry of taskEntries) {
    if (!entry.isFile() || !entry.name.endsWith(".md")) {
      continue;
    }
    const taskId = entry.name.slice(0, -3).toLowerCase();
    if (activeTaskIds.has(taskId)) {
      continue;
    }
    await rm(path.join(tasksDir, entry.name), { force: true });
  }

  await writeFocusProjection(nowFile, records, "now", nowProjection.legacy);
  await writeFocusProjection(nextFile, records, "next", nextProjection.legacy);

  const overviewLines = records.map(
    (record) => `- [${record.task_id}] ${record.priority} ${record.status} ${record.title}`,
  );
  await writeManagedLegacyBody(
    designerFile,
    designerOverviewHeader,
    overviewLines,
    designerOverviewLegacyTitle,
    designerLegacy,
  );

  return {
    projectedCount: records.length,
    taskIds: records.map((record) => record.task_id),
    records,
  };
}
