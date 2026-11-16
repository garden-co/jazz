import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import * as prettier from "prettier";
import { describe, expect, it } from "vitest";
import type { AppJson, PermissionsJson } from "./app-json.js";
import { parseExpr } from "./expr.js";
import { formatTs, layout, printPermissions, printSchemaTs } from "./index.js";
import { printMigrations } from "./migrations.js";

const REPO = join(__dirname, "..", "..", "..", "..");
const read = (path: string) => JSON.parse(readFileSync(path, "utf8"));

/** Every example app's directory holding jazz.app.json. */
function exampleApps(): string[] {
  const dirs: string[] = [];
  const walk = (dir: string, depth: number) => {
    if (existsSync(join(dir, "jazz.app.json"))) dirs.push(dir);
    if (depth >= 3) return;
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (entry.isDirectory() && entry.name !== "node_modules" && !entry.name.startsWith(".")) {
        walk(join(dir, entry.name), depth + 1);
      }
    }
  };
  walk(join(REPO, "examples"), 0);
  return dirs;
}

function permissionsOf(dir: string): PermissionsJson {
  const file = join(dir, "jazz.permissions.json");
  return existsSync(file) ? read(file) : { format: 1, tables: {} };
}

const app = (tables: AppJson["tables"]): AppJson => ({ format: 1, tables });
const print = (a: AppJson, tables: PermissionsJson["tables"]) =>
  printPermissions(a, { format: 1, tables });

describe("the expression reader", () => {
  it("reads every form of the grammar", () => {
    for (const source of [
      "userId = session.user",
      "not owner = null and (a = 1 or b != 'x''y')",
      "role in ['admin', 'editor']",
      "teamId in session.claims.teams",
      "tags contains 'x'",
      "exists chatMembers(chatId = this.id, userId = session.user)",
      "exists files as f(f.ownerId = session.user).folder(ownerId = session.user)",
      "allowedTo.read(chat)",
      "allowedTo.update(via parentId, maxDepth: 8)",
      "allowedTo.delete(via shares.todo)",
      "$createdBy.account = session.user",
      "n = int(5) and t = timestamp(1700000000000) and x = literal(null) and r = row(null, text('a'))",
      "exists (from files join folders as f on files.folderId = f.id where f.ownerId = session.user select files.id as id)",
      "exists (from recursive(seed: from folders where id = outer.parentId, step: from folders where id = frontier.parentId, frontier: parentId, dedupe: (id(row)), maxDepth: fixpoint) where ownerId = session.user)",
      "`order` = 1",
    ]) {
      expect(() => parseExpr(source), source).not.toThrow();
    }
    expect(() => parseExpr("a == 1")).toThrow(/`=`, not `==`/);
  });
});

describe("the permissions printer", () => {
  const todos = app({
    todos: {
      columns: { title: "string", ownerId: "uuid", done: "boolean", size: "bigint" },
      relations: { owner: { rel: "users", column: "ownerId" } },
    },
    users: { columns: { name: "string" }, relations: {} },
  });

  it("prints conditions, combinators and relations idiomatically", () => {
    const out = print(todos, {
      todos: {
        read: {
          anyOf: ["ownerId = session.user", "exists users(id = this.ownerId, name = 'root')"],
        },
        insert: "ownerId = session.user and done = false",
        update: {
          old: "ownerId = session.user",
          new: "exists todos(id = this.id, size <= this.size)",
        },
        delete: "size > 5",
      },
      users: { read: "true", insert: "false" },
    });
    expect(out.unsupported).toEqual([]);
    // A condition on the session that repeats becomes a helper.
    expect(out.code).toContain("const ownerIsMe = { ownerId: session.user };");
    expect(out.code).toContain(
      'read: (todo) => anyOf(ownerIsMe, todo.owner.where({ name: "root" })),',
    );
    expect(out.code).toContain("insert: { ownerId: session.user, done: false },");
    expect(out.code).toContain("updateIf: ownerIsMe,");
    expect(out.code).toContain("updateTo: (todo, old) => ({ size: gte(old.size) }),");
    expect(out.code).toContain("delete: { size: gt(5n) },");
    expect(out.code).toContain("read: true,\ninsert: false,");
  });

  it("factors any repeated check into one helper, literals that differ as parameters", () => {
    const shows = app({
      crew: { columns: { showId: "uuid", userId: "uuid", role: "string" } },
      notes: { columns: { showId: "uuid" } },
      cues: { columns: { showId: "uuid" } },
    });
    const crewOrChief = (role: string) =>
      `exists crew(showId = this.showId, userId = session.user) or exists crew(showId = this.showId, userId = session.user, role = '${role}')`;
    const out = print(shows, {
      notes: {
        read: crewOrChief("chief"),
        insert: crewOrChief("chief"),
        delete: crewOrChief("lead"),
      },
      cues: { read: crewOrChief("chief") },
    });
    expect(out.unsupported).toEqual([]);
    // One helper for the whole check; the role that differs between uses is a parameter.
    expect(out.code).toContain(
      'const isCrewOrChiefWithRole = (showId: s.Uuid, role: s.Value<"string">) => anyOf(exists.crew({ showId, userId: session.user }), exists.crew({ showId, userId: session.user, role }));',
    );
    expect(out.code).toContain('read: (cue) => isCrewOrChiefWithRole(cue.showId, "chief"),');
    expect(out.code).toContain('delete: (note) => isCrewOrChiefWithRole(note.showId, "lead"),');
  });

  it("prints named values by name", () => {
    const out = printPermissions(
      app({ chats: { columns: { kind: "string", role: "string" } } }),
      {
        format: 1,
        tables: { chats: { read: "kind = 'announcements' and role in ['admin', 'editor']" } },
      },
      {
        constants: new Map([
          ['"announcements"', "ANNOUNCEMENTS"],
          ['["admin", "editor"]', "editors"],
        ]),
      },
    );
    expect(out.code).toContain("read: { kind: ANNOUNCEMENTS, role: editors },");
    expect(out.constantsUsed.sort()).toEqual(["ANNOUNCEMENTS", "editors"]);
  });

  it("prints the relation IR", () => {
    const out = print(todos, {
      todos: {
        read: "exists (from todos join users as u on todos.ownerId = u.id where u.name = session.claims.name select todos.id as id)",
      },
    });
    expect(out.unsupported).toEqual([]);
    expect(out.code).toContain(
      'exists(from.todos().join(from.users("u"), { on: [["todos.ownerId", "u.id"]] }).where({ "u.name": session.claims.name }).select({ id: "todos.id" }))',
    );
  });

  it("reports what it can't print, by JSON path", () => {
    const out = print(todos, { todos: { read: { using: "true", withCheck: "true" } as never } });
    expect(out.unsupported.map((u) => u.path)).toEqual(["tables.todos.read"]);
  });

  it("prints every example's permissions", () => {
    const apps = exampleApps();
    expect(apps.length).toBeGreaterThan(20);
    for (const dir of apps) {
      const out = printSchemaTs(read(join(dir, "jazz.app.json")), permissionsOf(dir));
      expect(out.unsupported, dir).toEqual([]);
    }
  });

  it("prints chat-react as the agreed rendering", async () => {
    const dir = join(REPO, "examples", "chat-react");
    const out = printSchemaTs(read(join(dir, "jazz.app.json")), permissionsOf(dir));
    await expect(await formatTs(out.files["schema.ts"]!, { prettier })).toMatchFileSnapshot(
      "__snapshots__/chat-react.schema.ts.snap",
    );
  });

  it("prints big-label as the agreed rendering", async () => {
    const dir = join(REPO, "examples", "big-label");
    const out = printSchemaTs(read(join(dir, "jazz.app.json")), permissionsOf(dir));
    await expect(await formatTs(out.files["schema.ts"]!, { prettier })).toMatchFileSnapshot(
      "__snapshots__/big-label.schema.ts.snap",
    );
  });
});

describe("the layout without prettier", () => {
  it("is prettier's on every example", async () => {
    for (const dir of exampleApps()) {
      const out = printSchemaTs(read(join(dir, "jazz.app.json")), permissionsOf(dir));
      for (const [name, text] of Object.entries(out.files)) {
        expect(layout(text), `${dir}/${name}`).toBe(await formatTs(text, { prettier }));
      }
    }
  });
});

describe("the schema printer", () => {
  it("prints computed fields, transforms, indexes and history modes", () => {
    const dir = join(REPO, "crates", "jazz", "fixtures", "app-artifact", "shared-todos");
    const out = printSchemaTs(read(join(dir, "jazz.app.json")), permissionsOf(dir));
    expect(out.unsupported).toEqual([]);
    const schema = out.files["schema.ts"]!;
    expect(schema).toContain('shareCount: s.computed(s.int(), ["id"])');
    expect(schema).toContain(
      'meta: s.json({ type: "object" }).optional().transform("jsonText", s.string())',
    );
    expect(schema).toContain('.indexOnly(["owner"])');
    expect(schema).toContain('.history("latest")');
  });

  it("prints a migration stub per migration and a snapshot per version", async () => {
    const dir = join(REPO, "crates", "jazz", "fixtures", "app-artifact", "shared-todos");
    let n = 0;
    const out = await printMigrations(read(join(dir, "jazz.app.json")), {
      snapshot: (tables) => ({
        hash: `${++n}`.padStart(12, "0").padEnd(64, "f"),
        schema: { tables },
      }),
      start: new Date("2026-11-16T00:00:00Z"),
    });
    expect(out.unsupported).toEqual([]);
    expect(Object.keys(out.files).sort()).toEqual([
      "migrations/20261116T000002-000000000001-000000000002.ts",
      "migrations/snapshots/20261116T000000-000000000001.json",
      "migrations/snapshots/20261116T000001-000000000002.json",
    ]);
    const stub = out.files["migrations/20261116T000002-000000000001-000000000002.ts"]!;
    expect(stub).toContain('name: m.renameFrom("title"),');
    expect(stub).toContain("tags: m.add.array({ of: s.string(), default: [] }),");
    expect(stub).toContain('status: m.add.enum("open", "done", { default: null }),');
    expect(stub).toContain('todo: s.uuid().references("todos").optional()');
  });
});
