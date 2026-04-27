# Skill Issues Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `examples/skill-issues`, a CLI-first Jazz Cloud issue/idea manager with mandatory verified GitHub identity for writes, optional local UI, repo-local skill integration, and an explicit cutover from tracked Markdown todos.

**Architecture:** The example owns schema, permissions, CLI, verifier routes, UI, import/export, and tests. Domain parsing/formatting is pure TypeScript; persistence goes through a Jazz-backed repository; the CLI is the primary surface; the UI and GitHub verifier are served by `issues serve`. The repo-local `issues` skill delegates all operations to the CLI and never edits generated Markdown directly.

**Tech Stack:** TypeScript, Vitest, `tsx`, `jazz-tools`, `jazz-napi`, Express for the local server/verifier, React + Vite for the optional UI, GitHub device OAuth over `fetch`.

---

## File Structure

- `examples/skill-issues/package.json`: workspace package, scripts, dependencies, CLI bin.
- `examples/skill-issues/tsconfig.json`: TypeScript config for CLI/server/shared code.
- `examples/skill-issues/vitest.config.ts`: Vitest config.
- `examples/skill-issues/schema.ts`: Jazz tables for `items`, `itemStates`, and `users`.
- `examples/skill-issues/permissions.ts`: Jazz permissions enforcing verified-user writes and backend-only `users`.
- `examples/skill-issues/src/domain/types.ts`: shared item, state, user, and command result types.
- `examples/skill-issues/src/domain/markdown.ts`: import/export parser and formatter for current Markdown compatibility.
- `examples/skill-issues/src/domain/slugs.ts`: slug validation.
- `examples/skill-issues/src/config.ts`: environment/local config loading and saving.
- `examples/skill-issues/src/local-auth.ts`: local-first secret generation, storage, and proof token helpers.
- `examples/skill-issues/src/repository.ts`: Jazz-backed repository API used by CLI, server, and UI routes.
- `examples/skill-issues/src/cli.ts`: command parser and command implementations.
- `examples/skill-issues/bin/issues.js`: executable entrypoint.
- `examples/skill-issues/src/server/server.ts`: Express server for UI API and verifier routes.
- `examples/skill-issues/src/server/github.ts`: GitHub device OAuth and user lookup functions.
- `examples/skill-issues/src/ui/main.tsx`: UI entrypoint.
- `examples/skill-issues/src/ui/App.tsx`: UI screen for list/create/assign/status/export.
- `examples/skill-issues/src/ui/styles.css`: UI styling.
- `examples/skill-issues/index.html`: Vite HTML entry.
- `examples/skill-issues/tests/*.test.ts`: integration-oriented tests.
- `.agents/skills/issues/SKILL.md`: repo-local skill instructions.
- `.agents/skills/issues/agents/openai.yaml`: skill UI metadata.
- `.gitignore`: generated exports and local auth/config/data ignores.
- `CLAUDE.md` and `AGENTS.md`: Quick Capture instructions replaced with skill/CLI workflow.

## Task 1: Scaffold Package And Markdown Compatibility

**Files:**

- Create: `examples/skill-issues/package.json`
- Create: `examples/skill-issues/tsconfig.json`
- Create: `examples/skill-issues/vitest.config.ts`
- Create: `examples/skill-issues/src/domain/types.ts`
- Create: `examples/skill-issues/src/domain/slugs.ts`
- Create: `examples/skill-issues/src/domain/markdown.ts`
- Create: `examples/skill-issues/tests/markdown.test.ts`

- [ ] **Step 1: Write failing Markdown import/export tests**

Create `examples/skill-issues/tests/markdown.test.ts`:

```ts
import { mkdtemp, mkdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { exportMarkdownTodo, importMarkdownTodo } from "../src/domain/markdown.js";

async function tempRoot() {
  return mkdtemp(join(tmpdir(), "skill-issues-markdown-"));
}

describe("markdown compatibility", () => {
  it("imports existing ideas and issues using filename slugs and What sections", async () => {
    const root = await tempRoot();
    await mkdir(join(root, "todo/ideas/1_mvp"), { recursive: true });
    await mkdir(join(root, "todo/issues"), { recursive: true });
    await writeFile(
      join(root, "todo/ideas/1_mvp/explicit-indices.md"),
      "# Explicit Indices\n\n## What\n\nDeveloper-declared indices in the schema language.\n\n## Notes\n\nIgnored note.\n",
    );
    await writeFile(
      join(root, "todo/issues/reconnect-outbox-dedup.md"),
      "# Reconnect outbox dedup\n\n## What\n\nReconnect should not replay duplicate frames.\n\n## Priority\n\nhigh\n\n## Notes\n\nIgnored issue note.\n",
    );

    const items = await importMarkdownTodo(join(root, "todo"));

    expect(items).toEqual([
      {
        kind: "idea",
        slug: "explicit-indices",
        title: "Explicit Indices",
        description: "Developer-declared indices in the schema language.",
      },
      {
        kind: "issue",
        slug: "reconnect-outbox-dedup",
        title: "Reconnect outbox dedup",
        description: "Reconnect should not replay duplicate frames.",
      },
    ]);
  });

  it("exports ideas and issues to the current Markdown shape", async () => {
    const root = await tempRoot();

    await exportMarkdownTodo(join(root, "todo"), [
      {
        kind: "idea",
        slug: "explicit-indices",
        title: "Explicit Indices",
        description: "Developer-declared indices in the schema language.",
      },
      {
        kind: "issue",
        slug: "reconnect-outbox-dedup",
        title: "Reconnect outbox dedup",
        description: "Reconnect should not replay duplicate frames.",
      },
    ]);

    await expect(
      readFile(join(root, "todo/ideas/1_mvp/explicit-indices.md"), "utf8"),
    ).resolves.toBe(
      "# Explicit Indices\n\n## What\n\nDeveloper-declared indices in the schema language.\n\n## Notes\n\n",
    );
    await expect(
      readFile(join(root, "todo/issues/reconnect-outbox-dedup.md"), "utf8"),
    ).resolves.toBe(
      "# Reconnect outbox dedup\n\n## What\n\nReconnect should not replay duplicate frames.\n\n## Priority\n\nunknown\n\n## Notes\n\n",
    );
  });

  it("rejects duplicate slugs across ideas and issues", async () => {
    const root = await tempRoot();
    await mkdir(join(root, "todo/ideas/1_mvp"), { recursive: true });
    await mkdir(join(root, "todo/issues"), { recursive: true });
    await writeFile(join(root, "todo/ideas/1_mvp/same-slug.md"), "# Idea\n\n## What\n\nOne\n");
    await writeFile(join(root, "todo/issues/same-slug.md"), "# Issue\n\n## What\n\nTwo\n");

    await expect(importMarkdownTodo(join(root, "todo"))).rejects.toThrow(
      "Duplicate item slug: same-slug",
    );
  });
});
```

- [ ] **Step 2: Run the failing test**

Run: `pnpm --filter skill-issues test -- markdown.test.ts`

Expected: fails because package and domain files do not exist.

- [ ] **Step 3: Add package scaffold**

Create `examples/skill-issues/package.json`:

```json
{
  "name": "skill-issues",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "bin": {
    "issues": "./bin/issues.js"
  },
  "scripts": {
    "build": "tsc",
    "dev": "tsx watch src/cli.ts",
    "test": "vitest run",
    "serve": "tsx src/cli.ts serve"
  },
  "dependencies": {
    "@vitejs/plugin-react": "catalog:default",
    "express": "^4.18.2",
    "jazz-napi": "workspace:*",
    "jazz-tools": "workspace:*",
    "react": "19.2.4",
    "react-dom": "19.2.4",
    "vite": "catalog:default"
  },
  "devDependencies": {
    "@types/express": "^4.17.21",
    "@types/node": "^20.0.0",
    "@types/react": "^19.0.0",
    "@types/react-dom": "^19.0.0",
    "tsx": "^4.21.0",
    "typescript": "catalog:default",
    "vitest": "catalog:default"
  }
}
```

Create `examples/skill-issues/tsconfig.json`:

```json
{
  "extends": "../../tsconfig.json",
  "compilerOptions": {
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "target": "ES2022",
    "lib": ["ES2022", "DOM"],
    "jsx": "react-jsx",
    "outDir": "dist",
    "rootDir": ".",
    "types": ["node", "vitest/globals"],
    "skipLibCheck": true
  },
  "include": ["src", "tests", "schema.ts", "permissions.ts", "vite.config.ts", "vitest.config.ts"]
}
```

Create `examples/skill-issues/vitest.config.ts`:

```ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
  },
});
```

- [ ] **Step 4: Implement pure domain files**

Create `examples/skill-issues/src/domain/types.ts`:

```ts
export type ItemKind = "idea" | "issue";
export type ItemStatus = "open" | "in_progress" | "done";

export interface IssueItem {
  kind: ItemKind;
  title: string;
  description: string;
  slug: string;
}

export interface ItemState {
  itemSlug: string;
  status: ItemStatus;
  assigneeUserId?: string;
}

export interface VerifiedUser {
  id: string;
  githubUserId: string;
  githubLogin: string;
  verifiedAt: string;
}
```

`users.jazzUserId` is a persisted policy key, not part of the public `VerifiedUser` DTO. The repository derives `users.jazzUserId = VerifiedUser.id` when writing a user row and strips it when returning a `VerifiedUser`.

Create `examples/skill-issues/src/domain/slugs.ts`:

```ts
const SLUG_PATTERN = /^[a-z0-9][a-z0-9_-]*$/;

export function validateSlug(slug: string): string {
  if (!SLUG_PATTERN.test(slug)) {
    throw new Error(`Invalid item slug: ${slug}`);
  }
  return slug;
}
```

Create `examples/skill-issues/src/domain/markdown.ts` with:

```ts
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import type { IssueItem, ItemKind } from "./types.js";
import { validateSlug } from "./slugs.js";

function section(markdown: string, heading: string): string {
  const pattern = new RegExp(`^## ${heading}\\n+([\\s\\S]*?)(?=\\n## |\\n*$)`, "m");
  return markdown.match(pattern)?.[1]?.trim() ?? "";
}

function title(markdown: string, fallback: string): string {
  return markdown.match(/^# (.+)$/m)?.[1]?.trim() ?? fallback;
}

async function importFiles(dir: string, kind: ItemKind): Promise<IssueItem[]> {
  const entries = await readdir(dir, { withFileTypes: true }).catch(() => []);
  const items: IssueItem[] = [];
  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      items.push(...(await importFiles(path, kind)));
    } else if (entry.isFile() && entry.name.endsWith(".md")) {
      const slug = validateSlug(basename(entry.name, ".md"));
      const markdown = await readFile(path, "utf8");
      items.push({
        kind,
        slug,
        title: title(markdown, slug),
        description: section(markdown, "What"),
      });
    }
  }
  return items.sort((a, b) => a.slug.localeCompare(b.slug));
}

export async function importMarkdownTodo(todoDir: string): Promise<IssueItem[]> {
  const ideas = await importFiles(join(todoDir, "ideas"), "idea");
  const issues = await importFiles(join(todoDir, "issues"), "issue");
  const items = [...ideas, ...issues].sort((a, b) => a.slug.localeCompare(b.slug));
  const seen = new Set<string>();
  for (const item of items) {
    if (seen.has(item.slug)) throw new Error(`Duplicate item slug: ${item.slug}`);
    seen.add(item.slug);
  }
  return items;
}

function formatIdea(item: IssueItem): string {
  return `# ${item.title}\n\n## What\n\n${item.description}\n\n## Notes\n\n`;
}

function formatIssue(item: IssueItem): string {
  return `# ${item.title}\n\n## What\n\n${item.description}\n\n## Priority\n\nunknown\n\n## Notes\n\n`;
}

export async function exportMarkdownTodo(todoDir: string, items: IssueItem[]): Promise<void> {
  const seen = new Set<string>();
  for (const item of items) {
    validateSlug(item.slug);
    if (seen.has(item.slug)) throw new Error(`Duplicate item slug: ${item.slug}`);
    seen.add(item.slug);
    const file =
      item.kind === "idea"
        ? join(todoDir, "ideas", "1_mvp", `${item.slug}.md`)
        : join(todoDir, "issues", `${item.slug}.md`);
    await mkdir(join(file, ".."), { recursive: true });
    await writeFile(file, item.kind === "idea" ? formatIdea(item) : formatIssue(item));
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter skill-issues test -- markdown.test.ts`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add examples/skill-issues/package.json examples/skill-issues/tsconfig.json examples/skill-issues/vitest.config.ts examples/skill-issues/src/domain examples/skill-issues/tests/markdown.test.ts
git commit -m "feat: scaffold skill issues markdown compatibility"
```

## Task 2: Jazz Schema, Permissions, And Repository Contract

**Files:**

- Create: `examples/skill-issues/schema.ts`
- Create: `examples/skill-issues/permissions.ts`
- Create: `examples/skill-issues/src/repository.ts`
- Create: `examples/skill-issues/tests/repository.test.ts`

- [ ] **Step 1: Write failing repository/permission tests**

Create `examples/skill-issues/tests/repository.test.ts`:

```ts
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createJazzContext } from "jazz-tools/backend";
import { app } from "../schema.js";
import permissions from "../permissions.js";
import { createIssueRepository } from "../src/repository.js";

async function createTestRepository(userId: string) {
  const dataPath = join(await mkdtemp(join(tmpdir(), "skill-issues-repo-")), "db");
  const context = createJazzContext({
    appId: "skill-issues-test",
    app,
    permissions,
    driver: { type: "persistent", dataPath },
    env: "dev",
    userBranch: "main",
  });
  const backend = createIssueRepository(context.db(), app);
  const user = createIssueRepository(
    context.forSession({ user_id: userId, authMode: "local-first", claims: {} }),
    app,
  );
  return { backend, user };
}

describe("Jazz issue repository", () => {
  it("requires a verified user row before item writes", async () => {
    const { user } = await createTestRepository("alice-jazz-id");

    await expect(
      user.upsertItem({
        kind: "issue",
        slug: "policy-error-reasons",
        title: "Policy error reasons",
        description: "Expose structured policy denial reasons.",
      }),
    ).rejects.toThrow(/verified GitHub identity/i);
  });

  it("lets verified users create, list, assign, and update status", async () => {
    const { backend, user } = await createTestRepository("alice-jazz-id");
    await backend.upsertVerifiedUser({
      id: "alice-jazz-id",
      githubUserId: "1001",
      githubLogin: "alice",
      verifiedAt: "2026-04-27T16:00:00.000Z",
    });

    await user.upsertItem({
      kind: "issue",
      slug: "policy-error-reasons",
      title: "Policy error reasons",
      description: "Expose structured policy denial reasons.",
    });
    await user.assignMe("policy-error-reasons");
    await user.setStatus("policy-error-reasons", "done");

    await expect(user.listItems({})).resolves.toEqual([
      {
        kind: "issue",
        slug: "policy-error-reasons",
        title: "Policy error reasons",
        description: "Expose structured policy denial reasons.",
        state: {
          itemSlug: "policy-error-reasons",
          status: "done",
          assigneeUserId: "alice-jazz-id",
        },
        assignee: {
          id: "alice-jazz-id",
          githubUserId: "1001",
          githubLogin: "alice",
          verifiedAt: "2026-04-27T16:00:00.000Z",
        },
      },
    ]);
  });

  it("prevents regular users from writing verified user rows", async () => {
    const { user } = await createTestRepository("alice-jazz-id");

    await expect(
      user.upsertVerifiedUser({
        id: "alice-jazz-id",
        githubUserId: "1001",
        githubLogin: "alice",
        verifiedAt: "2026-04-27T16:00:00.000Z",
      }),
    ).rejects.toThrow();
  });
});
```

- [ ] **Step 2: Run the failing repository test**

Run: `pnpm --filter skill-issues test -- repository.test.ts`

Expected: fails because `schema.ts`, `permissions.ts`, and `src/repository.ts` do not exist.

- [ ] **Step 3: Implement schema and permissions**

Create `examples/skill-issues/schema.ts`:

```ts
import { schema as s } from "jazz-tools";

const schema = {
  users: s.table({
    jazzUserId: s.string(),
    githubUserId: s.string(),
    githubLogin: s.string(),
    verifiedAt: s.string(),
  }),
  items: s.table({
    kind: s.enum("idea", "issue"),
    title: s.string(),
    description: s.string(),
    slug: s.string(),
  }),
  itemStates: s.table({
    itemSlug: s.string(),
    status: s.enum("open", "in_progress", "done"),
    assigneeUserId: s.ref("users").optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
```

Create `examples/skill-issues/permissions.ts`:

```ts
import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.users.allowRead.always();
  policy.users.allowInsert.never();
  policy.users.allowUpdate.never();
  policy.users.allowDelete.never();

  policy.items.allowRead.always();
  policy.itemStates.allowRead.always();

  const isVerifiedUser = policy.users.exists.where({ jazzUserId: session.user_id });

  policy.items.allowInsert.where(isVerifiedUser);
  policy.items.allowUpdate.where(isVerifiedUser);
  policy.items.allowDelete.where(isVerifiedUser);

  policy.itemStates.allowInsert.where(isVerifiedUser);
  policy.itemStates.allowUpdate.where(isVerifiedUser);
  policy.itemStates.allowDelete.where(isVerifiedUser);
});
```

- [ ] **Step 4: Implement repository**

Create `examples/skill-issues/src/repository.ts`:

```ts
import type { App, Db } from "jazz-tools";
import type { IssueItem, ItemState, ItemStatus, VerifiedUser } from "./domain/types.js";
import { validateSlug } from "./domain/slugs.js";

export interface ListedItem extends IssueItem {
  state: ItemState;
  assignee?: VerifiedUser;
}

export interface ListFilters {
  kind?: "idea" | "issue";
  status?: ItemStatus;
}

export function createIssueRepository(db: Db, app: App<any>) {
  function toVerifiedUser(row: App["users"]["_rowType"]): VerifiedUser {
    return {
      id: row.id,
      githubUserId: row.githubUserId,
      githubLogin: row.githubLogin,
      verifiedAt: row.verifiedAt,
    };
  }

  async function currentUser(): Promise<VerifiedUser> {
    const session = db.getAuthState().session;
    if (!session?.user_id) throw new Error("A local-first Jazz identity is required.");
    const user = await db.one(app.users.where({ jazzUserId: session.user_id }));
    if (!user) throw new Error("A verified GitHub identity is required. Run issues auth github.");
    return toVerifiedUser(user);
  }

  async function stateFor(slug: string): Promise<ItemState> {
    const state = await db.one(app.itemStates.where({ itemSlug: slug }));
    return (state as ItemState | null) ?? { itemSlug: slug, status: "open" };
  }

  return {
    async upsertVerifiedUser(user: VerifiedUser): Promise<void> {
      const existing = await db.one(app.users.where({ id: user.id }));
      if (existing) {
        await db
          .update(app.users, user.id, {
            jazzUserId: user.id,
            githubUserId: user.githubUserId,
            githubLogin: user.githubLogin,
            verifiedAt: user.verifiedAt,
          })
          .wait({ tier: "local" });
      } else {
        await db
          .insert(app.users, {
            id: user.id,
            jazzUserId: user.id,
            githubUserId: user.githubUserId,
            githubLogin: user.githubLogin,
            verifiedAt: user.verifiedAt,
          })
          .wait({ tier: "local" });
      }
    },

    async upsertItem(item: IssueItem): Promise<void> {
      await currentUser();
      validateSlug(item.slug);
      const colliding = await db.one(app.items.where({ slug: item.slug }));
      if (colliding && colliding.kind !== item.kind)
        throw new Error(`Duplicate item slug: ${item.slug}`);
      if (colliding) {
        await db.update(app.items, colliding.id, item).wait({ tier: "local" });
      } else {
        await db.insert(app.items, item).wait({ tier: "local" });
        await db
          .insert(app.itemStates, { itemSlug: item.slug, status: "open" })
          .wait({ tier: "local" });
      }
    },

    async listItems(filters: ListFilters): Promise<ListedItem[]> {
      const items = await db.all(
        filters.kind ? app.items.where({ kind: filters.kind }) : app.items,
      );
      const users = await db.all(app.users);
      const byUserId = new Map(users.map((user: VerifiedUser) => [user.id, user]));
      const listed: ListedItem[] = [];
      for (const item of items as IssueItem[]) {
        const state = await stateFor(item.slug);
        if (filters.status && state.status !== filters.status) continue;
        listed.push({
          ...item,
          state,
          assignee: state.assigneeUserId ? byUserId.get(state.assigneeUserId) : undefined,
        });
      }
      return listed.sort((a, b) => a.slug.localeCompare(b.slug));
    },

    async getItem(slug: string): Promise<ListedItem | null> {
      return (await this.listItems({})).find((item) => item.slug === slug) ?? null;
    },

    async assignMe(slug: string): Promise<void> {
      const user = await currentUser();
      const item = await db.one(app.items.where({ slug }));
      if (!item) throw new Error(`Item not found: ${slug}`);
      const state = await db.one(app.itemStates.where({ itemSlug: slug }));
      const nextStatus = !state || state.status === "open" ? "in_progress" : state.status;
      if (state) {
        await db
          .update(app.itemStates, state.id, { assigneeUserId: user.id, status: nextStatus })
          .wait({ tier: "local" });
      } else {
        await db
          .insert(app.itemStates, {
            itemSlug: slug,
            assigneeUserId: user.id,
            status: "in_progress",
          })
          .wait({ tier: "local" });
      }
    },

    async setStatus(slug: string, status: ItemStatus): Promise<void> {
      await currentUser();
      const item = await db.one(app.items.where({ slug }));
      if (!item) throw new Error(`Item not found: ${slug}`);
      const state = await db.one(app.itemStates.where({ itemSlug: slug }));
      if (state) {
        await db.update(app.itemStates, state.id, { status }).wait({ tier: "local" });
      } else {
        await db.insert(app.itemStates, { itemSlug: slug, status }).wait({ tier: "local" });
      }
    },
  };
}
```

- [ ] **Step 5: Run repository tests**

Run: `pnpm --filter skill-issues test -- repository.test.ts`

Expected: PASS after adapting any `Db` type imports to the actual exported type names.

- [ ] **Step 6: Commit**

```bash
git add examples/skill-issues/schema.ts examples/skill-issues/permissions.ts examples/skill-issues/src/repository.ts examples/skill-issues/tests/repository.test.ts
git commit -m "feat: add skill issues Jazz repository"
```

## Task 3: CLI Config, Auth Init, And Command Routing

**Files:**

- Create: `examples/skill-issues/src/config.ts`
- Create: `examples/skill-issues/src/local-auth.ts`
- Create: `examples/skill-issues/src/cli.ts`
- Create: `examples/skill-issues/bin/issues.js`
- Create: `examples/skill-issues/tests/cli.test.ts`

- [ ] **Step 1: Write failing CLI tests**

Create `examples/skill-issues/tests/cli.test.ts`:

```ts
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { runCli } from "../src/cli.js";

async function tempHome() {
  return mkdtemp(join(tmpdir(), "skill-issues-cli-"));
}

describe("CLI", () => {
  it("initializes ignored local config and local-first secret", async () => {
    const home = await tempHome();
    const result = await runCli(["auth", "init"], {
      cwd: home,
      env: {
        SKILL_ISSUES_APP_ID: "app-id",
        SKILL_ISSUES_SERVER_URL: "https://cloud.example",
      },
    });

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("Initialized skill issues auth");
    const config = JSON.parse(await readFile(join(home, ".skill-issues/config.json"), "utf8"));
    expect(config.appId).toBe("app-id");
    expect(config.serverUrl).toBe("https://cloud.example");
    expect(typeof config.localFirstSecret).toBe("string");
  });

  it("rejects unknown commands with exit code 1", async () => {
    const result = await runCli(["wat"], { cwd: await tempHome(), env: {} });

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("Unknown command: wat");
  });
});
```

- [ ] **Step 2: Run failing CLI tests**

Run: `pnpm --filter skill-issues test -- cli.test.ts`

Expected: fails because CLI files do not exist.

- [ ] **Step 3: Implement config and local auth**

Create `examples/skill-issues/src/config.ts`:

```ts
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

export interface CliConfig {
  appId: string;
  serverUrl: string;
  verifierUrl?: string;
  localFirstSecret: string;
}

export function configPath(cwd: string): string {
  return join(cwd, ".skill-issues", "config.json");
}

export async function readConfig(cwd: string, env: NodeJS.ProcessEnv): Promise<CliConfig> {
  const fromFile = JSON.parse(await readFile(configPath(cwd), "utf8").catch(() => "{}"));
  const appId = env.SKILL_ISSUES_APP_ID ?? fromFile.appId;
  const serverUrl = env.SKILL_ISSUES_SERVER_URL ?? fromFile.serverUrl;
  const verifierUrl = env.SKILL_ISSUES_VERIFIER_URL ?? fromFile.verifierUrl;
  const localFirstSecret = env.SKILL_ISSUES_LOCAL_FIRST_SECRET ?? fromFile.localFirstSecret;
  if (!appId) throw new Error("SKILL_ISSUES_APP_ID is required.");
  if (!serverUrl) throw new Error("SKILL_ISSUES_SERVER_URL is required.");
  if (!localFirstSecret) throw new Error("Run issues auth init before using this command.");
  return { appId, serverUrl, verifierUrl, localFirstSecret };
}

export async function writeConfig(cwd: string, config: CliConfig): Promise<void> {
  await mkdir(join(cwd, ".skill-issues"), { recursive: true });
  await writeFile(configPath(cwd), `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
}
```

Create `examples/skill-issues/src/local-auth.ts`:

```ts
import { randomBytes } from "node:crypto";

export function generateLocalFirstSecret(): string {
  return randomBytes(32).toString("base64url");
}
```

- [ ] **Step 4: Implement CLI routing**

Create `examples/skill-issues/src/cli.ts`:

```ts
import { writeConfig } from "./config.js";
import { generateLocalFirstSecret } from "./local-auth.js";

export interface CliRuntime {
  cwd: string;
  env: NodeJS.ProcessEnv;
}

export interface CliResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

function ok(stdout: string): CliResult {
  return { exitCode: 0, stdout, stderr: "" };
}

function fail(stderr: string): CliResult {
  return { exitCode: 1, stdout: "", stderr };
}

export async function runCli(args: string[], runtime: CliRuntime): Promise<CliResult> {
  const [command, subcommand] = args;
  try {
    if (command === "auth" && subcommand === "init") {
      const appId = runtime.env.SKILL_ISSUES_APP_ID;
      const serverUrl = runtime.env.SKILL_ISSUES_SERVER_URL;
      if (!appId) return fail("SKILL_ISSUES_APP_ID is required.\n");
      if (!serverUrl) return fail("SKILL_ISSUES_SERVER_URL is required.\n");
      await writeConfig(runtime.cwd, {
        appId,
        serverUrl,
        verifierUrl: runtime.env.SKILL_ISSUES_VERIFIER_URL,
        localFirstSecret: generateLocalFirstSecret(),
      });
      return ok("Initialized skill issues auth.\n");
    }
    return fail(`Unknown command: ${command ?? ""}\n`);
  } catch (error) {
    return fail(`${(error as Error).message}\n`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const result = await runCli(process.argv.slice(2), { cwd: process.cwd(), env: process.env });
  process.stdout.write(result.stdout);
  process.stderr.write(result.stderr);
  process.exitCode = result.exitCode;
}
```

Create `examples/skill-issues/bin/issues.js`:

```js
#!/usr/bin/env node
import "../dist/src/cli.js";
```

- [ ] **Step 5: Run CLI tests**

Run: `pnpm --filter skill-issues test -- cli.test.ts`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add examples/skill-issues/src/config.ts examples/skill-issues/src/local-auth.ts examples/skill-issues/src/cli.ts examples/skill-issues/bin/issues.js examples/skill-issues/tests/cli.test.ts
git commit -m "feat: add skill issues CLI auth init"
```

## Task 4: CLI Data Commands

**Files:**

- Modify: `examples/skill-issues/src/cli.ts`
- Modify: `examples/skill-issues/src/config.ts`
- Create: `examples/skill-issues/src/db.ts`
- Create: `examples/skill-issues/tests/cli-data.test.ts`

- [ ] **Step 1: Write failing CLI data tests**

Create `examples/skill-issues/tests/cli-data.test.ts` using a test-only repository injection:

```ts
import { mkdtemp, mkdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { runCli, type CliDependencies } from "../src/cli.js";
import type { IssueItem } from "../src/domain/types.js";

function memoryRepo() {
  const items = new Map<string, IssueItem>();
  return {
    async upsertItem(item: IssueItem) {
      items.set(item.slug, item);
    },
    async listItems() {
      return [...items.values()]
        .sort((a, b) => a.slug.localeCompare(b.slug))
        .map((item) => ({
          ...item,
          state: { itemSlug: item.slug, status: "open" as const },
        }));
    },
    async getItem(slug: string) {
      const item = items.get(slug);
      return item ? { ...item, state: { itemSlug: slug, status: "open" as const } } : null;
    },
    async assignMe(slug: string) {
      if (!items.has(slug)) throw new Error(`Item not found: ${slug}`);
    },
    async setStatus(slug: string) {
      if (!items.has(slug)) throw new Error(`Item not found: ${slug}`);
    },
  };
}

async function run(args: string[], repo = memoryRepo()) {
  const cwd = await mkdtemp(join(tmpdir(), "skill-issues-cli-data-"));
  const deps: CliDependencies = { openRepository: async () => repo };
  return { cwd, repo, result: await runCli(args, { cwd, env: {} }, deps) };
}

describe("CLI data commands", () => {
  it("adds and lists an issue", async () => {
    const repo = memoryRepo();
    await run(
      [
        "add",
        "issue",
        "policy-error-reasons",
        "--title",
        "Policy error reasons",
        "--description",
        "Expose structured policy denial reasons.",
      ],
      repo,
    );

    const { result } = await run(["list"], repo);

    expect(result.stdout).toContain("issue policy-error-reasons open Policy error reasons");
  });

  it("imports and exports Markdown", async () => {
    const repo = memoryRepo();
    const cwd = await mkdtemp(join(tmpdir(), "skill-issues-import-export-"));
    await mkdir(join(cwd, "todo/issues"), { recursive: true });
    await writeFile(
      join(cwd, "todo/issues/reconnect-outbox-dedup.md"),
      "# Reconnect outbox dedup\n\n## What\n\nReconnect should not replay duplicate frames.\n",
    );
    const deps: CliDependencies = { openRepository: async () => repo };

    await expect(runCli(["import", "todo"], { cwd, env: {} }, deps)).resolves.toMatchObject({
      exitCode: 0,
    });
    await expect(runCli(["export", "exported"], { cwd, env: {} }, deps)).resolves.toMatchObject({
      exitCode: 0,
    });

    await expect(
      readFile(join(cwd, "exported/issues/reconnect-outbox-dedup.md"), "utf8"),
    ).resolves.toContain("Reconnect should not replay duplicate frames.");
  });
});
```

- [ ] **Step 2: Run failing data tests**

Run: `pnpm --filter skill-issues test -- cli-data.test.ts`

Expected: fails because `CliDependencies` and data command branches are missing.

- [ ] **Step 3: Add repository opener**

Create `examples/skill-issues/src/db.ts`:

```ts
import { createDb } from "jazz-tools";
import { createJazzContext } from "jazz-tools/backend";
import { app } from "../schema.js";
import permissions from "../permissions.js";
import { readConfig } from "./config.js";
import { createIssueRepository } from "./repository.js";

export async function openRepository(cwd: string, env: NodeJS.ProcessEnv) {
  const config = await readConfig(cwd, env);
  const db = await createDb({
    appId: config.appId,
    app,
    permissions,
    serverUrl: config.serverUrl,
    secret: config.localFirstSecret,
  });
  return createIssueRepository(db, app);
}

export async function openBackendRepository() {
  const appId = process.env.SKILL_ISSUES_APP_ID;
  const serverUrl = process.env.SKILL_ISSUES_SERVER_URL;
  const backendSecret = process.env.SKILL_ISSUES_BACKEND_SECRET;
  if (!appId) throw new Error("SKILL_ISSUES_APP_ID is required.");
  if (!serverUrl) throw new Error("SKILL_ISSUES_SERVER_URL is required.");
  if (!backendSecret) throw new Error("SKILL_ISSUES_BACKEND_SECRET is required.");
  const context = createJazzContext({
    appId,
    app,
    permissions,
    driver: { type: "memory" },
    serverUrl,
    backendSecret,
    env: "dev",
    userBranch: "main",
  });
  return createIssueRepository(context.db(), app);
}
```

- [ ] **Step 4: Extend CLI with data commands**

Update `examples/skill-issues/src/cli.ts`:

```ts
import { exportMarkdownTodo, importMarkdownTodo } from "./domain/markdown.js";
import { openRepository as defaultOpenRepository } from "./db.js";
import type { IssueItem, ItemKind, ItemStatus } from "./domain/types.js";
import { join } from "node:path";

export interface CliDependencies {
  openRepository?: typeof defaultOpenRepository;
}

function valueAfter(args: string[], name: string): string | undefined {
  const index = args.indexOf(name);
  return index === -1 ? undefined : args[index + 1];
}

function requireValue(value: string | undefined, label: string): string {
  if (!value) throw new Error(`${label} is required.`);
  return value;
}

function formatItem(item: any): string {
  return `${item.kind} ${item.slug} ${item.state.status} ${item.title}`;
}
```

Add command branches for:

```ts
const openRepo = deps.openRepository ?? defaultOpenRepository;

if (command === "add") {
  const kind = subcommand as ItemKind;
  if (kind !== "idea" && kind !== "issue")
    return fail(
      "Usage: issues add <idea|issue> <slug> --title <title> --description <description>\n",
    );
  const slug = requireValue(args[2], "slug");
  const title = requireValue(valueAfter(args, "--title"), "--title");
  const description = requireValue(valueAfter(args, "--description"), "--description");
  const repo = await openRepo(runtime.cwd, runtime.env);
  await repo.upsertItem({ kind, slug, title, description });
  return ok(`Saved ${kind} ${slug}.\n`);
}

if (command === "list") {
  const kind = valueAfter(args, "--kind") as ItemKind | undefined;
  const status = valueAfter(args, "--status") as ItemStatus | undefined;
  const repo = await openRepo(runtime.cwd, runtime.env);
  const items = await repo.listItems({ kind, status });
  return ok(`${items.map(formatItem).join("\n")}${items.length ? "\n" : ""}`);
}

if (command === "show") {
  const slug = requireValue(subcommand, "slug");
  const repo = await openRepo(runtime.cwd, runtime.env);
  const item = await repo.getItem(slug);
  if (!item) return fail(`Item not found: ${slug}\n`);
  return ok(`${formatItem(item)}\n\n${item.description}\n`);
}

if (command === "assign" && valueAfter(args, "--me") !== undefined) {
  const slug = requireValue(subcommand, "slug");
  const repo = await openRepo(runtime.cwd, runtime.env);
  await repo.assignMe(slug);
  return ok(`Assigned ${slug} to current user.\n`);
}

if (command === "status") {
  const slug = requireValue(subcommand, "slug");
  const status = requireValue(args[2], "status") as ItemStatus;
  const repo = await openRepo(runtime.cwd, runtime.env);
  await repo.setStatus(slug, status);
  return ok(`Set ${slug} to ${status}.\n`);
}

if (command === "import") {
  const dir = requireValue(subcommand, "directory");
  const repo = await openRepo(runtime.cwd, runtime.env);
  const items = await importMarkdownTodo(join(runtime.cwd, dir));
  for (const item of items) await repo.upsertItem(item);
  return ok(`Imported ${items.length} items.\n`);
}

if (command === "export") {
  const dir = requireValue(subcommand, "directory");
  const repo = await openRepo(runtime.cwd, runtime.env);
  const items = await repo.listItems({});
  await exportMarkdownTodo(
    join(runtime.cwd, dir),
    items.map(
      (item: any): IssueItem => ({
        kind: item.kind,
        slug: item.slug,
        title: item.title,
        description: item.description,
      }),
    ),
  );
  return ok(`Exported ${items.length} items.\n`);
}
```

- [ ] **Step 5: Run data tests**

Run: `pnpm --filter skill-issues test -- cli-data.test.ts`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add examples/skill-issues/src/cli.ts examples/skill-issues/src/config.ts examples/skill-issues/src/db.ts examples/skill-issues/tests/cli-data.test.ts
git commit -m "feat: add skill issues data commands"
```

## Task 5: GitHub Verification Routes And Auth Command

**Files:**

- Create: `examples/skill-issues/src/server/github.ts`
- Create: `examples/skill-issues/src/server/server.ts`
- Create: `examples/skill-issues/tests/support/http.ts`
- Modify: `examples/skill-issues/src/cli.ts`
- Modify: `examples/skill-issues/src/local-auth.ts`
- Create: `examples/skill-issues/tests/github-verifier.test.ts`

- [ ] **Step 1: Write failing verifier tests**

Create `examples/skill-issues/tests/github-verifier.test.ts`:

```ts
import { describe, expect, it, vi } from "vitest";
import { createVerifierApp } from "../src/server/server.js";
import { requestJson } from "./support/http.js";

describe("GitHub verifier", () => {
  it("writes a verified user after GitHub and Jazz proofs are valid", async () => {
    const upsertVerifiedUser = vi.fn(async () => {});
    const app = createVerifierApp({
      github: {
        exchangeDeviceCode: async () => ({ accessToken: "gh-token" }),
        fetchUser: async () => ({ id: "1001", login: "alice" }),
      },
      verifyJazzProof: async () => ({ jazzUserId: "alice-jazz-id" }),
      openBackendRepository: async () => ({ upsertVerifiedUser }),
    });

    const response = await requestJson(app, "POST", "/auth/github/complete", {
      deviceCode: "device-code",
      jazzProof: "proof",
    });

    expect(response.statusCode).toBe(200);
    expect(upsertVerifiedUser).toHaveBeenCalledWith({
      id: "alice-jazz-id",
      githubUserId: "1001",
      githubLogin: "alice",
      verifiedAt: expect.any(String),
    });
  });
});
```

Create `examples/skill-issues/tests/support/http.ts`:

```ts
import type { Express } from "express";
import type { Server } from "node:http";

export async function requestJson(
  app: Express,
  method: string,
  path: string,
  body?: unknown,
): Promise<{ statusCode: number; body: string }> {
  const server: Server = await new Promise((resolve) => {
    const listening = app.listen(0, () => resolve(listening));
  });
  try {
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Expected an ephemeral TCP port.");
    const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
      method,
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    return { statusCode: response.status, body: await response.text() };
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}
```

- [ ] **Step 2: Run failing verifier tests**

Run: `pnpm --filter skill-issues test -- github-verifier.test.ts`

Expected: fails because server files do not exist.

- [ ] **Step 3: Implement GitHub helpers**

Create `examples/skill-issues/src/server/github.ts`:

```ts
export interface GitHubDeviceStart {
  device_code: string;
  user_code: string;
  verification_uri: string;
  interval: number;
}

export interface GitHubToken {
  accessToken: string;
}

export interface GitHubUser {
  id: string;
  login: string;
}

export async function startDeviceAuthorization(clientId: string): Promise<GitHubDeviceStart> {
  const response = await fetch("https://github.com/login/device/code", {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/json" },
    body: JSON.stringify({ client_id: clientId, scope: "read:user" }),
  });
  if (!response.ok) throw new Error(`GitHub device authorization failed: ${response.status}`);
  return (await response.json()) as GitHubDeviceStart;
}

export async function exchangeDeviceCode(input: {
  clientId: string;
  clientSecret: string;
  deviceCode: string;
}): Promise<GitHubToken> {
  const response = await fetch("https://github.com/login/oauth/access_token", {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/json" },
    body: JSON.stringify({
      client_id: input.clientId,
      client_secret: input.clientSecret,
      device_code: input.deviceCode,
      grant_type: "urn:ietf:params:oauth:grant-type:device_code",
    }),
  });
  if (!response.ok) throw new Error(`GitHub token exchange failed: ${response.status}`);
  const body = (await response.json()) as { access_token?: string; error?: string };
  if (!body.access_token) throw new Error(body.error ?? "GitHub token exchange returned no token.");
  return { accessToken: body.access_token };
}

export async function fetchGitHubUser(accessToken: string): Promise<GitHubUser> {
  const response = await fetch("https://api.github.com/user", {
    headers: { Accept: "application/vnd.github+json", Authorization: `Bearer ${accessToken}` },
  });
  if (!response.ok) throw new Error(`GitHub user lookup failed: ${response.status}`);
  const body = (await response.json()) as { id: number; login: string };
  return { id: String(body.id), login: body.login };
}
```

- [ ] **Step 4: Implement verifier app and CLI auth command**

Create `examples/skill-issues/src/server/server.ts` with an Express app exposing:

```ts
import express from "express";
import { exchangeDeviceCode, fetchGitHubUser } from "./github.js";
import { openBackendRepository } from "../db.js";
import { verifyLocalFirstIdentityProof } from "jazz-napi";

export function createVerifierApp(
  deps = {
    github: {
      exchangeDeviceCode: async (deviceCode: string) =>
        exchangeDeviceCode({
          clientId: process.env.GITHUB_CLIENT_ID ?? "",
          clientSecret: process.env.GITHUB_CLIENT_SECRET ?? "",
          deviceCode,
        }),
      fetchUser: fetchGitHubUser,
    },
    verifyJazzProof: async (proof: string) => {
      const result = verifyLocalFirstIdentityProof(proof, "skill-issues-github");
      if (!result.ok) throw new Error(result.error);
      return { jazzUserId: result.id };
    },
    openBackendRepository,
  },
) {
  const app = express();
  app.use(express.json());
  app.post("/auth/github/complete", async (req, res, next) => {
    try {
      const { deviceCode, jazzProof } = req.body as { deviceCode?: string; jazzProof?: string };
      if (!deviceCode || !jazzProof) {
        res.status(400).json({ error: "deviceCode and jazzProof are required" });
        return;
      }
      const token = await deps.github.exchangeDeviceCode(deviceCode);
      const githubUser = await deps.github.fetchUser(token.accessToken);
      const proof = await deps.verifyJazzProof(jazzProof);
      const repo = await deps.openBackendRepository();
      await repo.upsertVerifiedUser({
        id: proof.jazzUserId,
        githubUserId: githubUser.id,
        githubLogin: githubUser.login,
        verifiedAt: new Date().toISOString(),
      });
      res.json({ id: proof.jazzUserId, githubLogin: githubUser.login });
    } catch (error) {
      next(error);
    }
  });
  return app;
}
```

Update `src/local-auth.ts` to expose `createLocalFirstProof(secret: string): string` using `mintLocalFirstToken` from `jazz-napi`. Use audience string `skill-issues-github`:

```ts
import { randomBytes } from "node:crypto";
import { mintLocalFirstToken } from "jazz-napi";

export function generateLocalFirstSecret(): string {
  return randomBytes(32).toString("base64url");
}

export function createLocalFirstProof(secret: string): string {
  return mintLocalFirstToken(secret, "skill-issues-github", 60);
}
```

Update `src/cli.ts` so `issues auth github --verifier-url <url>`:

1. Reads config.
2. Starts GitHub device authorization with `GITHUB_CLIENT_ID`.
3. Prints the verification URI and user code.
4. Polls or waits for completion according to the GitHub response interval.
5. Sends `{ deviceCode, jazzProof }` to `${verifierUrl}/auth/github/complete`.
6. Prints `Verified GitHub user <login>.`

- [ ] **Step 5: Run verifier tests**

Run: `pnpm --filter skill-issues test -- github-verifier.test.ts`

Expected: PASS with GitHub and Jazz proof dependencies mocked.

- [ ] **Step 6: Commit**

```bash
git add examples/skill-issues/src/server examples/skill-issues/src/local-auth.ts examples/skill-issues/src/cli.ts examples/skill-issues/tests/github-verifier.test.ts examples/skill-issues/tests/support/http.ts
git commit -m "feat: add skill issues GitHub verifier"
```

## Task 6: Local UI Server

**Files:**

- Create: `examples/skill-issues/index.html`
- Create: `examples/skill-issues/vite.config.ts`
- Create: `examples/skill-issues/src/ui/main.tsx`
- Create: `examples/skill-issues/src/ui/App.tsx`
- Create: `examples/skill-issues/src/ui/styles.css`
- Modify: `examples/skill-issues/src/server/server.ts`
- Modify: `examples/skill-issues/src/cli.ts`
- Create: `examples/skill-issues/tests/server-api.test.ts`

- [ ] **Step 1: Write failing server API test**

Create `examples/skill-issues/tests/server-api.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { createSkillIssuesServer } from "../src/server/server.js";
import { requestJson } from "./support/http.js";

describe("skill issues server API", () => {
  it("lists items through the shared repository", async () => {
    const server = createSkillIssuesServer({
      openRepository: async () => ({
        listItems: async () => [
          {
            kind: "issue",
            slug: "policy-error-reasons",
            title: "Policy error reasons",
            description: "Expose structured policy denial reasons.",
            state: { itemSlug: "policy-error-reasons", status: "open" },
          },
        ],
      }),
    });

    const response = await requestJson(server, "GET", "/api/items");

    expect(response.statusCode).toBe(200);
    expect(JSON.parse(response.body)).toEqual([
      {
        kind: "issue",
        slug: "policy-error-reasons",
        title: "Policy error reasons",
        description: "Expose structured policy denial reasons.",
        state: { itemSlug: "policy-error-reasons", status: "open" },
      },
    ]);
  });
});
```

- [ ] **Step 2: Run failing server API test**

Run: `pnpm --filter skill-issues test -- server-api.test.ts`

Expected: fails because `createSkillIssuesServer` is missing.

- [ ] **Step 3: Add API routes**

Update `examples/skill-issues/src/server/server.ts` with `createSkillIssuesServer`:

```ts
export function createSkillIssuesServer(deps = { openRepository }) {
  const app = createVerifierApp();
  app.get("/api/items", async (_req, res, next) => {
    try {
      const repo = await deps.openRepository(process.cwd(), process.env);
      res.json(await repo.listItems({}));
    } catch (error) {
      next(error);
    }
  });
  app.post("/api/items", async (req, res, next) => {
    try {
      const repo = await deps.openRepository(process.cwd(), process.env);
      await repo.upsertItem(req.body);
      res.status(201).json({ ok: true });
    } catch (error) {
      next(error);
    }
  });
  app.post("/api/items/:slug/assign-me", async (req, res, next) => {
    try {
      const repo = await deps.openRepository(process.cwd(), process.env);
      await repo.assignMe(req.params.slug);
      res.json({ ok: true });
    } catch (error) {
      next(error);
    }
  });
  app.post("/api/items/:slug/status", async (req, res, next) => {
    try {
      const repo = await deps.openRepository(process.cwd(), process.env);
      await repo.setStatus(req.params.slug, req.body.status);
      res.json({ ok: true });
    } catch (error) {
      next(error);
    }
  });
  return app;
}
```

- [ ] **Step 4: Add UI**

Create a compact React UI in `src/ui/App.tsx` with filters, list, create form, assign button, and status select. Use real API calls:

```tsx
async function api(path: string, init?: RequestInit) {
  const response = await fetch(path, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}
```

Render rows with kind, slug, title, status, assignee GitHub login, `Assign me`, and status controls. Keep styles restrained and functional.

- [ ] **Step 5: Wire `issues serve`**

Update `src/cli.ts`:

```ts
if (command === "serve") {
  const { createSkillIssuesServer } = await import("./server/server.js");
  const port = Number(valueAfter(args, "--port") ?? runtime.env.PORT ?? "4242");
  const app = createSkillIssuesServer();
  await new Promise<void>((resolve) => app.listen(port, resolve));
  return ok(`Skill issues server running at http://localhost:${port}\n`);
}
```

For the executable CLI path, keep the process alive after starting the server. In tests, use the returned app directly instead of invoking the long-running command.

- [ ] **Step 6: Run server tests**

Run: `pnpm --filter skill-issues test -- server-api.test.ts`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add examples/skill-issues/index.html examples/skill-issues/vite.config.ts examples/skill-issues/src/ui examples/skill-issues/src/server/server.ts examples/skill-issues/src/cli.ts examples/skill-issues/tests/server-api.test.ts
git commit -m "feat: add skill issues local UI server"
```

## Task 7: Repo Skill And Cutover Instructions

**Files:**

- Create: `.agents/skills/issues/SKILL.md`
- Create: `.agents/skills/issues/agents/openai.yaml`
- Modify: `AGENTS.md`
- Modify: `CLAUDE.md`
- Modify: `.gitignore`
- Create: `examples/skill-issues/tests/cutover-docs.test.ts`

- [ ] **Step 1: Write failing docs/cutover tests**

Create `examples/skill-issues/tests/cutover-docs.test.ts`:

```ts
import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

describe("repo cutover instructions", () => {
  it("replaces Markdown quick capture instructions with the issues skill", async () => {
    const agents = await readFile("../../AGENTS.md", "utf8");
    const claude = await readFile("../../CLAUDE.md", "utf8");

    for (const text of [agents, claude]) {
      expect(text).toContain("Use the `issues` skill");
      expect(text).not.toContain("After every write to `todo/`, run `bash scripts/update-todo.sh`");
      expect(text).not.toContain("Ideas → `todo/ideas/{priority}/{idea-name}.md`");
      expect(text).not.toContain("Issues → `todo/issues/{issue-name}.md`");
    }
  });

  it("ignores generated Markdown exports and local skill issues state", async () => {
    const gitignore = await readFile("../../.gitignore", "utf8");

    expect(gitignore).toContain("todo/");
    expect(gitignore).toContain(".skill-issues/");
    expect(gitignore).toContain("examples/skill-issues/.skill-issues/");
  });
});
```

- [ ] **Step 2: Run failing cutover docs test**

Run: `pnpm --filter skill-issues test -- cutover-docs.test.ts`

Expected: fails because instructions still reference Markdown quick capture.

- [ ] **Step 3: Add `issues` skill**

Create `.agents/skills/issues/SKILL.md`:

```md
---
name: issues
description: Use for creating, listing, assigning, updating, importing, or exporting Jazz repo issues and ideas. Replaces direct Markdown todo capture.
---

# Issues

Use the `examples/skill-issues` CLI for all issue and idea operations.

Do not create or edit `todo/ideas/**/*.md` or `todo/issues/**/*.md` as source files. Markdown under `todo/` is generated export output only.

## Commands

- Initialize local auth: `pnpm --filter skill-issues exec issues auth init`
- Verify GitHub identity: `pnpm --filter skill-issues exec issues auth github --verifier-url <url>`
- Add issue: `pnpm --filter skill-issues exec issues add issue <slug> --title "<title>" --description "<description>"`
- Add idea: `pnpm --filter skill-issues exec issues add idea <slug> --title "<title>" --description "<description>"`
- List: `pnpm --filter skill-issues exec issues list`
- Show: `pnpm --filter skill-issues exec issues show <slug>`
- Self-assign: `pnpm --filter skill-issues exec issues assign <slug> --me`
- Set status: `pnpm --filter skill-issues exec issues status <slug> <open|in_progress|done>`
- Export Markdown: `pnpm --filter skill-issues exec issues export todo`

## Workflow

For capture requests, create a Jazz item with status `open`.

For assignment requests, call `issues assign <slug> --me`.

For status updates, call `issues status <slug> <status>`.

If a write command fails because the user is not verified, tell the user to run GitHub verification. Do not fall back to Markdown files.
```

Create `.agents/skills/issues/agents/openai.yaml`:

```yaml
display_name: Issues
short_description: Manage repo issues and ideas through the Jazz-backed skill-issues CLI.
default_prompt: Use the issues skill to capture, list, assign, or update repo issues and ideas.
```

- [ ] **Step 4: Replace Quick Capture instructions**

In both `AGENTS.md` and `CLAUDE.md`, replace the existing Quick Capture section with:

```md
## Quick Capture: Ideas & Issues

Use the `issues` skill for ideas, bugs, and focused problems.

The Jazz Cloud-backed skill issues system is the source of truth. Do not write new source Markdown under `todo/ideas/`, `todo/issues/`, or `todo/projects/`. Markdown exports under `todo/` are generated compatibility output only.

For capture, create a Jazz item with status `open`. For self-assignment, use the skill/CLI assignment command. For status updates, use `open`, `in_progress`, or `done`.
```

- [ ] **Step 5: Update `.gitignore`**

Add:

```gitignore
# Skill issues generated/exported state
todo/
.skill-issues/
examples/skill-issues/.skill-issues/
```

- [ ] **Step 6: Run cutover docs test**

Run: `pnpm --filter skill-issues test -- cutover-docs.test.ts`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add .agents/skills/issues AGENTS.md CLAUDE.md .gitignore examples/skill-issues/tests/cutover-docs.test.ts
git commit -m "feat: add issues skill instructions"
```

## Task 8: Migration And Removal Of Tracked Markdown Todo System

**Files:**

- Delete: `todo/ideas/**`
- Delete: `todo/issues/**`
- Delete or move: `todo/projects/**`
- Delete: `TODO.md`
- Delete: `scripts/update-todo.sh`
- Create: `examples/skill-issues/tests/current-import-fixture.test.ts`

- [ ] **Step 1: Write current import fixture test before deleting files**

Create `examples/skill-issues/tests/current-import-fixture.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { importMarkdownTodo } from "../src/domain/markdown.js";

describe("current repo Markdown import", () => {
  it("imports the current tracked ideas and issues before cutover", async () => {
    const items = await importMarkdownTodo("../../todo");

    expect(items.length).toBeGreaterThan(30);
    expect(items.some((item) => item.slug === "explicit-indices" && item.kind === "idea")).toBe(
      true,
    );
    expect(
      items.some(
        (item) =>
          item.slug === "better-auth-generalize-unique-field-enforcement" && item.kind === "issue",
      ),
    ).toBe(true);
  });
});
```

- [ ] **Step 2: Run current import fixture test**

Run: `pnpm --filter skill-issues test -- current-import-fixture.test.ts`

Expected: PASS before deleting tracked Markdown.

- [ ] **Step 3: Review `todo/projects/`**

Run:

```bash
find todo/projects -maxdepth 2 -type f | sort
```

For each file, decide one of:

- Convert the active work intent into `issues add idea` or `issues add issue`.
- Move durable long-form docs into `docs/superpowers/specs/` or another committed spec/docs location.

Record the conversion decisions in the implementation PR description. Do not silently delete unique project intent.

- [ ] **Step 4: Run real import into Jazz Cloud**

After `issues auth init` and `issues auth github` are configured for the target Jazz Cloud app, run:

```bash
pnpm --filter skill-issues exec issues import todo
pnpm --filter skill-issues exec issues list
```

Expected: list includes current ideas and issues, including `explicit-indices` and `better-auth-generalize-unique-field-enforcement`.

- [ ] **Step 5: Remove tracked Markdown todo system**

Run:

```bash
git rm -r todo TODO.md scripts/update-todo.sh
```

If any `todo/projects/` document was moved in Step 3, include the destination docs in the same commit.

- [ ] **Step 6: Verify export output is ignored**

Run:

```bash
pnpm --filter skill-issues exec issues export todo
git status --short --ignored todo
rm -rf todo
```

Expected: exported `todo/` appears as ignored output, not tracked changes.

- [ ] **Step 7: Run tests**

Run:

```bash
pnpm --filter skill-issues test
pnpm --filter skill-issues build
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add examples/skill-issues/tests/current-import-fixture.test.ts
git add docs
git commit -m "chore: cut over todo tracking to skill issues"
```

Keep the deletions staged for this commit.

## Task 9: Final Verification

**Files:**

- No new files expected.

- [ ] **Step 1: Run package tests**

Run:

```bash
pnpm --filter skill-issues test
```

Expected: PASS.

- [ ] **Step 2: Run package build**

Run:

```bash
pnpm --filter skill-issues build
```

Expected: PASS.

- [ ] **Step 3: Run relevant repo checks**

Run:

```bash
pnpm --filter skill-issues test
pnpm --filter skill-issues build
pnpm format:check
```

Expected: PASS.

- [ ] **Step 4: Manual CLI smoke**

With a configured dev Jazz Cloud app and verifier URL:

```bash
pnpm --filter skill-issues exec issues auth init
pnpm --filter skill-issues exec issues auth github --verifier-url "$SKILL_ISSUES_VERIFIER_URL"
pnpm --filter skill-issues exec issues add issue smoke-test --title "Smoke test" --description "Verify CLI write path."
pnpm --filter skill-issues exec issues assign smoke-test --me
pnpm --filter skill-issues exec issues status smoke-test done
pnpm --filter skill-issues exec issues show smoke-test
```

Expected: `show` prints `issue smoke-test done Smoke test`.

- [ ] **Step 5: Inspect git state**

Run:

```bash
git status --short
```

Expected: only intentional changes are present.
