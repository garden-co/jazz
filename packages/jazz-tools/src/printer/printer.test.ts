import { existsSync, readdirSync, readFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as prettier from "prettier";
import { describe, expect, it } from "vitest";
import { generateApp } from "../dev/project.js";
import { readablePermissions } from "../permissions/dsl.js";
import * as s from "../schema-builders.js";
import { readableSchema } from "../schema-dsl/define-schema.js";
import {
  permissions as encryptedPermissions,
  schema as encryptedSchema,
} from "./__fixtures__/encrypted-permissions.js";
import type { AppJson, PermissionsJson } from "./app-json.js";
import { printTs } from "./cli.js";
import { parseExpr } from "./expr.js";
import { formatTs, layout, printPermissions, printSchemaTs } from "./index.js";
import { printMigrations } from "./migrations.js";
import { expandDefs } from "./defs.js";

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
    // One helper for the whole check, taking values (so every table can use it); the
    // role that differs between uses is a parameter. Its structural name would be
    // too long to read, so it gets a short one for the author to rename.
    expect(out.code).toContain(
      'const rule1 = (showId: s.Uuid, role: s.Value<"string">) => anyOf(exists.crew({ showId, userId: session.user }), exists.crew({ showId, userId: session.user, role }));',
    );
    expect(out.code).toContain('read: (cue) => rule1(cue.showId, "chief"),');
    expect(out.code).toContain('delete: (note) => rule1(note.showId, "lead"),');
  });

  it("names parameters after the column they fill and types enum literals as their union", () => {
    const bands = app({
      members: { columns: { bandId: "uuid", userId: "uuid", role: { enum: ["admin", "player"] } } },
      gigs: { columns: { bandId: "uuid" } },
      songs: { columns: { bandId: "uuid" } },
    });
    const member = (role: string) =>
      `exists members(bandId = this.bandId, userId = session.user, role = '${role}')`;
    const out = print(bands, {
      gigs: { read: member("admin") },
      songs: { read: member("player") },
    });
    expect(out.code).toContain(
      'const isMemberWithRole = (bandId: s.Uuid, role: "admin" | "player") => exists.members({ bandId, userId: session.user, role });',
    );
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

  it("round-trips isMember against the encrypted schema", () => {
    const original = readablePermissions(encryptedPermissions)!;
    const out = print(
      JSON.parse(JSON.stringify({ format: 1, ...readableSchema(encryptedSchema)! })),
      original.tables as PermissionsJson["tables"],
    );
    expect(out.unsupported).toEqual([]);
    expect(out.code).toContain('isMember("rooms", room.id)');
    expect(out.code).toContain('isMember("rooms", message.roomId)');
    const printed: unknown = new Function(
      "schema",
      out.code.replace("export const permissions", "const permissions") + "\nreturn permissions;",
    )(encryptedSchema);
    expect(readablePermissions(printed)).toEqual(original);
  });

  it("round-trips isMember from a plain table to a space defined by another table", () => {
    const room = { rel: "rooms", column: "roomId" };
    const original = app({
      canvases: { columns: { roomId: "uuid" }, relations: { room } },
      messages: {
        columns: { roomId: "uuid", text: "bytes", __e2ee_eq_text: "bytes" },
        relations: { room },
        encryption: {
          scope: "rooms",
          space: "roomId",
          columns: { text: "string?" },
          equality: ["text"],
        },
      },
      rooms: { columns: { name: "string" } },
    });
    const permissions: PermissionsJson = {
      format: 1,
      tables: {
        canvases: {
          read: "exists __e2ee_members(scopeId = table_id('rooms'), identifier = this.roomId, accountId = session.user)",
        },
        messages: {},
        rooms: {},
      },
    };

    const out = printSchemaTs(original, permissions);
    expect(out.unsupported).toEqual([]);
    expect(out.files["schema.ts"]).toContain('isMember("rooms", canvas.roomId)');
    const printed: { schema: unknown; permissions: unknown } = new Function(
      "s",
      out.files["schema.ts"]!.replace(/^import .*$/m, "").replaceAll("export const", "const") +
        "\nreturn { schema, permissions };",
    )(s);
    expect(readablePermissions(printed.permissions)?.tables).toEqual(permissions.tables);
    expect({ format: 1, ...readableSchema(printed.schema) }).toEqual(original);
  });

  // Deploy recognises the members rule with its conditions in any order
  // (protocol R13), so the printer does too: each order prints isMember,
  // which compiles back to the order isMember emits.
  const MEMBER_CONDITIONS = [
    "scopeId = table_id('rooms')",
    "identifier = this.roomId",
    "accountId = session.user",
  ];
  const orders = (items: string[]): string[][] =>
    items.length <= 1
      ? [items]
      : items.flatMap((item, i) =>
          orders(items.filter((_, j) => j !== i)).map((rest) => [item, ...rest]),
        );
  it.each(orders(MEMBER_CONDITIONS).map((order) => [order.join(", ")]))(
    "prints isMember for the members rule as exists __e2ee_members(%s)",
    (conditions) => {
      const room = { rel: "rooms", column: "roomId" };
      const original = app({
        canvases: { columns: { roomId: "uuid" }, relations: { room } },
        rooms: {
          columns: { name: "bytes" },
          encryption: { scope: "rooms", space: "id", columns: { name: "string" } },
        },
      });
      const read = (body: string) => `exists __e2ee_members(${body})`;
      const out = printSchemaTs(original, {
        format: 1,
        tables: { canvases: { read: read(conditions) }, rooms: {} },
      });
      expect(out.unsupported).toEqual([]);
      expect(out.files["schema.ts"]).toContain('isMember("rooms", canvas.roomId)');
      const printed: { permissions: unknown } = new Function(
        "s",
        out.files["schema.ts"]!.replace(/^import .*$/m, "").replaceAll("export const", "const") +
          "\nreturn { schema, permissions };",
      )(s);
      expect(readablePermissions(printed.permissions)?.tables).toEqual({
        canvases: { read: read(MEMBER_CONDITIONS.join(", ")) },
        rooms: {},
      });
    },
  );

  it.each([
    [
      "a repeated column",
      "scopeId = table_id('rooms'), identifier = this.roomId, scopeId = table_id('rooms')",
    ],
    ["another column", "scopeId = table_id('rooms'), identifier = this.roomId, id = session.user"],
  ])("doesn't print isMember for three conditions with %s", (_, conditions) => {
    const out = print(todos, {
      todos: { read: `exists __e2ee_members(${conditions})` },
    });
    expect(out.unsupported).toEqual([
      {
        path: "tables.todos.read",
        text: `exists __e2ee_members(${conditions})`,
        reason: "Printing table_id outside isMember is not supported yet",
      },
    ]);
  });

  it("reports table_id outside the members rule by JSON path", () => {
    const comparison = "ownerId = table_id('users')";
    const nearMiss =
      "exists __e2ee_members(scopeId = table_id('todos'), identifier = this.id, accountId = session.user, id = this.id)";
    const out = print(todos, { todos: { read: comparison, insert: nearMiss } });
    const reason = "Printing table_id outside isMember is not supported yet";
    expect(out.unsupported).toEqual([
      { path: "tables.todos.read", text: comparison, reason },
      { path: "tables.todos.insert", text: nearMiss, reason },
    ]);
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

describe("defs", () => {
  it("expand where they are called, recursively, without touching exists or hops", () => {
    const out = expandDefs({
      format: 1,
      defs: {
        "shared($todo, $user)": "exists shares(todo = $todo, user = $user)",
        "mineOrShared($todo)": { anyOf: ["owner = session.user", "shared($todo, session.user)"] },
      },
      tables: {
        todos: {
          read: "mineOrShared(this.id)",
          update: {
            old: "true",
            new: "exists todos(id = this.id).owner(name = 'x') and shared(this.id, session.user)",
          },
        },
      },
    });
    expect(out.defs).toBeUndefined();
    expect(out.tables.todos!.read).toBe(
      "(anyOf(owner = session.user, (exists shares(todo = this.id, user = session.user))))",
    );
    expect(out.tables.todos!.update).toEqual({
      old: "true",
      new: "exists todos(id = this.id).owner(name = 'x') and (exists shares(todo = this.id, user = session.user))",
    });
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

  it("reports app-wide settings and unknown encryption fields", () => {
    const rooms = { columns: { name: "bytes", nameText: "string?" } };
    const encryption = {
      space: "id",
      scope: "rooms",
      columns: { name: { type: "string", from: "nameText" } },
      backfill: ["name"],
    };
    const out = printSchemaTs(
      {
        ...app({ rooms: { ...rooms, encryption } as AppJson["tables"][string] }),
        encryption: { mechanisms: {} },
      } as AppJson,
      { format: 1, tables: { rooms: {} } },
    );
    expect(out.unsupported).toEqual([
      {
        path: "tables.rooms.encryption.backfill",
        text: '["name"]',
        reason: "Unknown encryption key backfill. Expected space, scope, columns or equality.",
      },
      {
        path: "tables.rooms.encryption.columns.name.from",
        text: '"nameText"',
        reason:
          "Unknown encrypted column field from. Expected a type declaration, sealedType or default.",
      },
      {
        path: "encryption.mechanisms",
        text: "{}",
        reason: "Printing this app-wide encryption setting is not supported yet",
      },
    ]);
  });

  it("prints a pinned encrypted enum without history and generates the same pin", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-print-enum-"));
    const path = join(dir, "jazz.app.json");
    const status = { type: { enum: ["draft", "sent"] }, sealedType: { enum: ["draft"] } };
    try {
      await writeFile(
        path,
        JSON.stringify(
          app({
            rooms: {
              columns: { status: "bytes" },
              encryption: { space: "id", scope: "rooms", columns: { status } },
            },
          }),
        ),
      );
      await writeFile(
        join(dir, "jazz.permissions.json"),
        JSON.stringify({
          format: 1,
          tables: {
            rooms: {
              read: "exists __e2ee_members(scopeId = table_id('rooms'), identifier = this.id, accountId = session.user)",
            },
          },
        }),
      );
      const files = await printTs({ dir });
      expect(files).toContain("schema.ts");
      const sidecar = files.find((name) => name.endsWith(".encryption.json"));
      expect(sidecar).toBeDefined();
      expect(read(join(dir, sidecar!)).tables.rooms.columns.status).toEqual(status);
      expect(readFileSync(join(dir, "schema.ts"), "utf8")).toContain(
        's.enum("draft", "sent").encrypted()',
      );
      await generateApp({ cwd: dir, force: true });
      expect(read(path).tables.rooms.encryption.columns.status).toEqual(status);
    } finally {
      await rm(dir, { force: true, recursive: true });
    }
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
