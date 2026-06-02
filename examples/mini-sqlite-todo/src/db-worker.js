import init, { MiniJazzRuntime } from "./generated/mini-jazz-sqlite-wasm/mini_jazz_sqlite_wasm.js";

const PAGE_SIZE = 10;
const GENERATED_PROJECT_COUNT = 100;
const SEEDED_TODOS_PER_PROJECT = 10000;
const GENERATED_LABELS = ["work", "home", "urgent", "later", "bug", "idea", "docs", "release"];
const SORT_COLUMNS = {
  date: "$createdAt",
  name: "title",
};

const USERS = [
  { id: "user-alice", name: "Alice" },
  { id: "user-bob", name: "Bob" },
  { id: "user-cara", name: "Cara" },
];

const GROUPS = [
  { id: "group-engineering", name: "Engineering" },
  { id: "group-company", name: "Company" },
  { id: "group-design", name: "Design" },
  { id: "group-support", name: "Support" },
];

const GROUP_MEMBERS = [
  { id: "group-member-alice-engineering", member: "user:user-alice", group: "group-engineering" },
  { id: "group-member-alice-design", member: "user:user-alice", group: "group-design" },
  { id: "group-member-bob-engineering", member: "user:user-bob", group: "group-engineering" },
  { id: "group-member-cara-support", member: "user:user-cara", group: "group-support" },
  {
    id: "group-member-engineering-company",
    member: "group:group-engineering",
    group: "group-company",
  },
  { id: "group-member-design-company", member: "group:group-design", group: "group-company" },
  { id: "group-member-support-company", member: "group:group-support", group: "group-company" },
];

const BASE_PROJECTS = [
  {
    id: "todo-list",
    title: "Alice inbox",
    members: ["user:user-alice"],
  },
  {
    id: "project-launch-plan",
    title: "Launch plan",
    members: ["group:group-engineering"],
  },
  {
    id: "project-company-strategy",
    title: "Company strategy",
    members: ["group:group-company"],
  },
  {
    id: "project-design-polish",
    title: "Design polish",
    members: ["group:group-design"],
  },
  {
    id: "project-bob-private",
    title: "Bob private",
    members: ["user:user-bob"],
  },
  {
    id: "project-support-rotation",
    title: "Support rotation",
    members: ["group:group-support"],
  },
];

const GENERATED_MEMBERS = [
  "group:group-engineering",
  "group:group-company",
  "group:group-design",
  "group:group-support",
  "user:user-alice",
  "user:user-bob",
  "user:user-cara",
];

const SEED_USER_ID = "seed";
let db;
let dbName;
let nodeId;
let activeUserId = USERS[0].id;
let selectedProjectId = null;
let labelsById = new Map();
let showTableStats = false;
let filters = {
  search: "",
  labelIds: [],
  sortField: "date",
  sortDir: "desc",
};

self.onmessage = async ({ data }) => {
  try {
    if (data.type === "init") {
      await init();
      dbName = data.dbName;
      nodeId = data.nodeId;
      activeUserId = data.user ?? activeUserId;
      await openDbAs(SEED_USER_ID, { trusted: true });
      await seedDirectory();
      await openDbAs(activeUserId);
      refreshLabelCache();
    } else if (data.type === "setUser") {
      activeUserId = data.user;
      selectedProjectId = null;
      await openDbAs(activeUserId);
      refreshLabelCache();
      filters = sanitizeFilters(filters);
    } else if (data.type === "setProject") {
      selectedProjectId = data.projectId;
    } else if (data.type === "setTableStats") {
      showTableStats = Boolean(data.enabled);
    } else if (data.type === "add") {
      const scope = visibleScope();
      const projectId = visibleProjectId(data.projectId ?? selectedProjectId, scope);
      if (!projectId) {
        throw new Error("No visible project selected");
      }
      const id = `todo-${crypto.randomUUID()}`;
      db.insertRow("todos", id, {
        title: data.title,
        done: false,
        project: projectId,
      });
      addTodoLabels(id, data.labels);
    } else if (data.type === "setFilters") {
      filters = sanitizeFilters(data.filters);
    } else if (data.type === "generate") {
      const startedAt = performance.now();
      ensureLabels(GENERATED_LABELS);
      const projects = ensureGeneratedProjects();
      for (let i = 0; i < data.count; i++) {
        const project = projects[i % projects.length];
        const todoId = `todo-generated-${crypto.randomUUID()}`;
        const todoLabels = labelsForGeneratedTodo(i);
        db.insertRow("todos", todoId, {
          title: generatedTitle(i, todoLabels),
          done: false,
          project: project.id,
        });
        addTodoLabels(todoId, todoLabels);
        if ((i + 1) % 1000 === 0) {
          postMessage({ type: "progress", generated: i + 1, total: data.count });
          await new Promise((resolve) => setTimeout(resolve));
        }
      }
      postState(performance.now() - startedAt);
      return;
    } else if (data.type === "toggle") {
      if (visibleTodo(data.id)) {
        db.updateRow("todos", data.id, { done: data.done });
      }
    } else if (data.type === "rename") {
      const title = String(data.title ?? "").trim();
      if (title && visibleTodo(data.id)) {
        db.updateRow("todos", data.id, { title });
      }
    } else if (data.type === "delete") {
      const todo = visibleTodo(data.id);
      if (todo?.created_by === activeUserId) {
        deleteTodoLabels(data.id);
        db.deleteRow("todos", data.id);
      } else if (todo) {
        db.deleteRow("todos", data.id);
      }
    }
    postState();
  } catch (error) {
    postMessage({ type: "error", message: error.message ?? String(error) });
  }
};

async function openDbAs(userId, { trusted = false } = {}) {
  db?.free?.();
  db = trusted
    ? await MiniJazzRuntime.openTrustedTodoOpfs(dbName, nodeId)
    : await MiniJazzRuntime.openTodoOpfs(dbName, nodeId, userId);
}

async function seedDirectory() {
  refreshLabelCache();
  const userIds = rowIds("users");
  for (const user of USERS) {
    insertMissing("users", user.id, { name: user.name }, userIds);
  }

  const groupIds = rowIds("groups");
  for (const group of GROUPS) {
    insertMissing("groups", group.id, { name: group.name }, groupIds);
  }

  const groupMemberIds = rowIds("group_members");
  for (const member of GROUP_MEMBERS) {
    insertMissing(
      "group_members",
      member.id,
      groupMemberValues(member.member, member.group),
      groupMemberIds,
    );
  }

  const projectIds = rowIds("projects");
  const projectMemberIds = rowIds("project_members");
  const projects = [...BASE_PROJECTS, ...generatedProjects()];
  for (const project of projects) {
    insertMissing("projects", project.id, { title: project.title }, projectIds);
    ensureProjectMembers(project, projectMemberIds);
  }

  ensureLabels(GENERATED_LABELS);
  await seedProjectTodos(projects);
}

function ensureGeneratedProjects() {
  return generatedProjects();
}

function generatedProjects() {
  const projects = [];
  for (let i = 0; i < GENERATED_PROJECT_COUNT; i++) {
    const member = GENERATED_MEMBERS[i % GENERATED_MEMBERS.length];
    projects.push({
      id: `project-generated-${String(i + 1).padStart(3, "0")}`,
      title: `Generated ${String(i + 1).padStart(3, "0")}`,
      members: [member],
    });
  }
  return projects;
}

function ensureProjectMembers(project, projectMemberIds) {
  for (const member of project.members) {
    insertMissing(
      "project_members",
      `project-member-${project.id}-${slug(member)}`,
      projectMemberValues(project.id, member),
      projectMemberIds,
    );
  }
}

function groupMemberValues(member, group) {
  const ref = splitMemberRef(member);
  return {
    user: ref.user,
    member_group: ref.group,
    group,
  };
}

function projectMemberValues(project, member) {
  const ref = splitMemberRef(member);
  return {
    project,
    user: ref.user,
    group: ref.group,
  };
}

function splitMemberRef(member) {
  if (member.startsWith("user:")) {
    return { user: member.slice("user:".length), group: null };
  }
  if (member.startsWith("group:")) {
    return { user: null, group: member.slice("group:".length) };
  }
  return { user: null, group: null };
}

function insertMissing(table, id, values, ids) {
  if (ids.has(id)) return;
  db.insertRow(table, id, values);
  ids.add(id);
}

async function seedProjectTodos(projects) {
  const total = projects.length * SEEDED_TODOS_PER_PROJECT;
  let seen = 0;

  for (const project of projects) {
    const finalTodoId = seededTodoId(project.id, SEEDED_TODOS_PER_PROJECT);
    if (db.one({ table: "todos", conditions: [{ column: "id", op: "eq", value: finalTodoId }] })) {
      seen += SEEDED_TODOS_PER_PROJECT;
      postMessage({ type: "progress", generated: seen, total });
      continue;
    }

    const authors = authorsForProject(project);
    const rowsByAuthor = new Map();
    for (let i = 1; i <= SEEDED_TODOS_PER_PROJECT; i++) {
      const author = authors[(i - 1) % authors.length];
      const rows = rowsByAuthor.get(author) ?? [];
      rows.push({
        id: seededTodoId(project.id, i),
        values: {
          title: seededTodoTitle(project, i),
          done: false,
          project: project.id,
        },
      });
      rowsByAuthor.set(author, rows);
    }

    for (const [author, rows] of rowsByAuthor) {
      db.upsertRowsAsUser(author, "todos", rows);
      seen += rows.length;
      postMessage({ type: "progress", generated: seen, total });
      await new Promise((resolve) => setTimeout(resolve));
    }
  }

  postMessage({ type: "progress", generated: total, total });
}

function authorsForProject(project) {
  const authors = [];
  const seen = new Set();
  for (const member of project.members) {
    for (const userId of userIdsForMember(member)) {
      if (seen.has(userId)) continue;
      seen.add(userId);
      authors.push(userId);
    }
  }
  return authors.length ? authors : [USERS[0].id];
}

function userIdsForMember(member, seenGroups = new Set()) {
  if (member.startsWith("user:")) return [member.slice("user:".length)];
  if (!member.startsWith("group:")) return [];

  const groupId = member.slice("group:".length);
  if (seenGroups.has(groupId)) return [];
  const nextSeenGroups = new Set(seenGroups);
  nextSeenGroups.add(groupId);

  return GROUP_MEMBERS.filter((row) => row.group === groupId).flatMap((row) =>
    userIdsForMember(row.member, nextSeenGroups),
  );
}

function seededTodoId(projectId, index) {
  return `todo-seeded-${projectId}-${String(index).padStart(5, "0")}`;
}

function seededTodoTitle(project, index) {
  return `${project.title} task ${String(index).padStart(5, "0")}`;
}

function rowIds(table) {
  return new Set(db.readRows(table).map((row) => row.id));
}

function userName(userId) {
  return USERS.find((user) => user.id === userId)?.name ?? userId;
}

function visibleScope() {
  const startedAt = performance.now();
  const projectQuery = {
    table: "projects",
    orderBy: [["title", "asc"]],
  };
  const projects = db.query(projectQuery).map((row) => ({ id: row.id, title: row.values.title }));
  const groups = db
    .query({
      table: "groups",
      orderBy: [["name", "asc"]],
    })
    .map((row) => ({ id: row.id, name: row.values.name }));
  const projectIds = projects.map((project) => project.id);

  return {
    groups,
    projects,
    projectIds,
    projectIdSet: new Set(projectIds),
    visibilityMs: performance.now() - startedAt,
  };
}

function visibleProjectId(candidate, scope) {
  if (candidate && scope.projectIdSet.has(candidate)) return candidate;
  return scope.projects[0]?.id ?? null;
}

function visibleTodo(id) {
  return db.one({
    table: "todos",
    conditions: [{ column: "id", op: "eq", value: id }],
  });
}

function postState(generateMs) {
  const scope = visibleScope();
  selectedProjectId = visibleProjectId(selectedProjectId, scope);

  const todoStartedAt = performance.now();
  const selectedTodoIds = todoIdsForSelectedLabels(filters.labelIds);
  const rows = selectedTodoIds?.length === 0 ? [] : db.query(buildTodoQuery(selectedTodoIds));
  const projectTitles = new Map(scope.projects.map((project) => [project.id, project.title]));
  const todoLabels = labelsByTodoId(rows.map((row) => row.id));
  const todos = rows.map((row) => ({
    id: row.id,
    title: row.values.title,
    done: row.values.done,
    projectId: row.values.project,
    projectTitle: projectTitles.get(row.values.project) ?? row.values.project,
    createdBy: row.created_by,
    createdByName: userName(row.created_by),
    canRename: row.created_by === activeUserId,
    canDelete: row.created_by === activeUserId,
    labels: todoLabels.get(row.id) ?? [],
    txId: row.tx_id,
  }));
  const queryMs = performance.now() - todoStartedAt;
  const tableStats = showTableStats
    ? {
        currentRows: db.storageStats().current_rows,
        visibilityMs: scope.visibilityMs,
        queryMs,
      }
    : null;

  postMessage({
    type: "state",
    activeUserId,
    users: USERS,
    groups: scope.groups,
    projects: scope.projects,
    selectedProjectId,
    filters,
    labels: sortedLabels(),
    todos,
    showTableStats,
    tableStats,
    generateMs,
  });
}

function buildTodoQuery(todoIds) {
  const conditions = [{ column: "done", op: "eq", value: false }];
  const search = filters.search.trim();

  if (search) {
    conditions.push({ column: "title", op: "contains", value: search });
  }
  if (todoIds) {
    conditions.push({ column: "id", op: "in", value: todoIds });
  }

  return {
    table: "todos",
    conditions,
    includes: {},
    orderBy: [[SORT_COLUMNS[filters.sortField], filters.sortDir]],
    limit: PAGE_SIZE,
  };
}

function addTodoLabels(todoId, labelNames) {
  const labels = ensureLabels(labelNames);
  for (const label of labels) {
    db.insertRow("todo_labels", `${todoId}-${label.id}`, {
      todo: todoId,
      label: label.id,
    });
  }
}

function ensureLabels(labelNames) {
  const labels = [];
  const seen = new Set();
  for (const rawName of labelNames ?? []) {
    const name = normalizeLabelName(rawName);
    if (!name || seen.has(name)) continue;
    seen.add(name);
    const id = labelIdForName(name);
    if (!labelsById.has(id)) {
      db.insertRow("labels", id, { name });
      labelsById.set(id, { id, name });
    }
    labels.push(labelsById.get(id));
  }
  return labels;
}

function refreshLabelCache() {
  labelsById = new Map(
    db.readRows("labels").map((row) => [row.id, { id: row.id, name: row.values.name }]),
  );
}

function sortedLabels() {
  return Array.from(labelsById.values()).sort((left, right) => left.name.localeCompare(right.name));
}

function deleteTodoLabels(todoId) {
  for (const row of db.query({
    table: "todo_labels",
    conditions: [{ column: "todo", op: "eq", value: todoId }],
    includes: {},
  })) {
    db.deleteRow("todo_labels", row.id);
  }
}

function labelsByTodoId(todoIds) {
  const labels = new Map();
  if (todoIds.length === 0) return labels;
  const rows = db.query({
    table: "todo_labels",
    conditions: [{ column: "todo", op: "in", value: todoIds }],
    includes: {},
  });
  for (const row of rows) {
    const label = labelsById.get(row.values.label);
    if (!label) continue;
    const todoLabels = labels.get(row.values.todo) ?? [];
    todoLabels.push(label);
    labels.set(row.values.todo, todoLabels);
  }
  for (const todoLabels of labels.values()) {
    todoLabels.sort((left, right) => left.name.localeCompare(right.name));
  }
  return labels;
}

function todoIdsForSelectedLabels(labelIds) {
  if (!labelIds.length) return null;

  let intersection;
  for (const labelId of labelIds) {
    const ids = new Set(
      db
        .query({
          table: "todo_labels",
          conditions: [{ column: "label", op: "eq", value: labelId }],
          includes: {},
        })
        .map((row) => row.values.todo),
    );

    if (!intersection) {
      intersection = ids;
    } else {
      intersection = new Set([...intersection].filter((id) => ids.has(id)));
    }

    if (intersection.size === 0) return [];
  }

  return [...intersection];
}

function sanitizeFilters(nextFilters = {}) {
  const labelIds = Array.isArray(nextFilters.labelIds)
    ? nextFilters.labelIds.filter((id) => labelsById.has(id))
    : [];
  const sortField = nextFilters.sortField === "name" ? "name" : "date";
  const sortDir = nextFilters.sortDir === "asc" ? "asc" : "desc";
  return {
    search: String(nextFilters.search ?? "").slice(0, 80),
    labelIds,
    sortField,
    sortDir,
  };
}

function labelsForGeneratedTodo(index) {
  const labels = [GENERATED_LABELS[index % GENERATED_LABELS.length]];
  if (index % 3 === 0) labels.push(GENERATED_LABELS[(index + 3) % GENERATED_LABELS.length]);
  return labels;
}

function generatedTitle(index, labels) {
  const topic = index % 5 === 0 ? "ship" : index % 5 === 1 ? "review" : "note";
  return `Todo ${String(index + 1).padStart(6, "0")} ${topic} ${labels.join(" ")}`;
}

function normalizeLabelName(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replaceAll(/\s+/g, "-")
    .replaceAll(/[^a-z0-9_-]/g, "")
    .slice(0, 32);
}

function labelIdForName(name) {
  return `label-${name}`;
}

function slug(value) {
  return value
    .replace(/[^a-z0-9]+/gi, "-")
    .replace(/^-|-$/g, "")
    .toLowerCase();
}
