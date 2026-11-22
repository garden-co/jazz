import { existsSync, readFileSync } from "node:fs";
import { readdir, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../../tests/cli/fixtures.js";
import { writeAppFiles } from "../dev/codegen-napi.js";
import { createAppMigration, generateApp } from "../dev/project.js";

const { createWorkspace } = createCliFixtures(import.meta.url);

/** The hand-written encrypted app the code generators are tested with. */
const CODEGEN_FIXTURE = resolve(
  __dirname,
  "../../../../crates/jazz-codegen/tests/apps/encrypted/jazz.app.json",
);

/** A project with `schema.ts` (and its permissions) as given, run through `generate`. */
async function project(schema: string): Promise<string> {
  const { root } = await createWorkspace();
  await writeFile(join(root, "schema.ts"), schema);
  return root;
}

async function generated(root: string): Promise<{ app: string; permissions: string }> {
  await generateApp({ cwd: root });
  return {
    app: await readFile(join(root, "jazz.app.json"), "utf8"),
    permissions: await readFile(join(root, "jazz.permissions.json"), "utf8"),
  };
}

/**
 * The Rust writer's text of a hand-written app file, with the same
 * `$generated` marker as `generated`: what `generate` must print for a
 * `schema.ts` that declares the same app.
 */
async function written(handWritten: string, files: { app: string; permissions: string }) {
  const by = (JSON.parse(files.app) as { $generated: { by: string } }).$generated.by;
  return (await writeAppFiles(handWritten, files.permissions, { generated: { by } })).app;
}

function withoutMarker(text: string): unknown {
  const { $generated: _marker, ...rest } = JSON.parse(text) as Record<string, unknown>;
  return rest;
}

describe("declaring encryption in schema.ts", () => {
  it("generates the hand-written encrypted fixture app", async () => {
    const root = await project(`
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  projects: s.table({ name: s.string().encrypted(), ownerId: s.uuid() }, {}, { space: "id" }),
  tasks: s.table(
    {
      projectId: s.uuid(),
      title: s.string().encrypted({ equality: true }),
      notes: s.string().optional().encrypted(),
      done: s.boolean(),
    },
    { project: s.rel("projects", "projectId") },
    { space: "project" },
  ),
});

const { definePermissions, isMember } = schema;
export const permissions = definePermissions({
  projects: { read: (project) => isMember("projects", project.id) },
  tasks: { read: (task) => isMember("projects", task.projectId) },
});
`);
    const files = await generated(root);
    const handWritten = readFileSync(CODEGEN_FIXTURE, "utf8");
    expect(files.app).toBe(await written(handWritten, files));
    // The same app as written by hand: the writer only orders an entry's columns.
    expect(withoutMarker(files.app)).toEqual(JSON.parse(handWritten));
  });

  it("stores every type the example app encrypts as bytes, its plaintext type in the entry", async () => {
    const root = await project(`
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  rooms: s.table({ name: s.string() }),
  messages: s.table(
    {
      roomId: s.uuid(),
      text: s.string().encrypted(),
      attachmentName: s.string().optional().encrypted(),
      attachment: s.bytes().optional().encrypted(),
      pinned: s.boolean().default(false).encrypted(),
    },
    { room: s.rel("rooms", "roomId") },
    { space: "room" },
  ),
  readMarkers: s.table(
    { roomId: s.uuid(), userId: s.uuid(), lastReadAt: s.timestamp().encrypted() },
    { room: s.rel("rooms", "roomId") },
    { space: "room" },
  ),
});

const { definePermissions, isMember, session } = schema;
export const permissions = definePermissions({
  rooms: { read: (room) => isMember("rooms", room.id) },
  messages: { read: (message) => isMember("rooms", message.roomId) },
  readMarkers: { read: (marker) => ({ userId: session.user }) },
});
`);
    const files = await generated(root);
    const handWritten = JSON.stringify({
      $schema: "https://jazz.tools/schemas/jazz.app.v1.json",
      tables: {
        rooms: { columns: { name: "string" } },
        messages: {
          columns: {
            roomId: "uuid",
            text: "bytes",
            attachmentName: "bytes",
            attachment: "bytes",
            pinned: "bytes",
          },
          relations: { room: { rel: "rooms", column: "roomId" } },
          encryption: {
            space: "roomId",
            scope: "rooms",
            columns: {
              text: "string",
              attachmentName: "string?",
              attachment: "bytes?",
              pinned: { type: "boolean", default: false },
            },
          },
        },
        readMarkers: {
          columns: { roomId: "uuid", userId: "uuid", lastReadAt: "bytes" },
          relations: { room: { rel: "rooms", column: "roomId" } },
          encryption: { space: "roomId", scope: "rooms", columns: { lastReadAt: "timestamp" } },
        },
      },
    });
    expect(files.app).toBe(await written(handWritten, files));
  });

  it("generates an app without encryption as before: no entry, no members table", async () => {
    const root = await project(`
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({ notes: s.table({ text: s.string() }) });
export const permissions = schema.definePermissions({ notes: { read: true } });
`);
    const files = await generated(root);
    expect(withoutMarker(files.app)).toEqual({
      $schema: "https://jazz.tools/schemas/jazz.app.v1.json",
      tables: { notes: { columns: { text: "string" } } },
    });
  });

  it("lets migrations create start the history at an encrypted first version", async () => {
    const root = await project(`
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  rooms: s.table({ name: s.string().encrypted() }, {}, { space: "id" }),
});
const { definePermissions, isMember } = schema;
export const permissions = definePermissions({
  rooms: { read: (room) => isMember("rooms", room.id) },
});
`);
    const created = await createAppMigration({ cwd: root, name: "init" });
    expect(created.changed).toBe(true);
    expect(created.file).toBeUndefined();
    expect(created.snapshot && existsSync(created.snapshot)).toBe(true);
    const files = await generated(root);
    expect(JSON.parse(files.app).tables.rooms.encryption).toEqual({
      space: "id",
      scope: "rooms",
      columns: { name: "string" },
    });
    // The schema is unchanged since: nothing to create.
    expect((await createAppMigration({ cwd: root, name: "again" })).changed).toBe(false);
  });
});

/** A room-chat schema.ts: `rooms` plain, `messages` in the rooms' spaces with `text` encrypted. */
function chat(options: { rooms?: string; messages?: string; text?: string } = {}): string {
  return `
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  rooms: s.table({ name: s.string()${options.rooms ?? ""} }),
  messages: s.table(
    { roomId: s.uuid(), text: ${options.text ?? "s.string().encrypted()"}${options.messages ?? ""} },
    { room: s.rel("rooms", "roomId") },
    { space: "room" },
  ),
});

const { definePermissions, isMember } = schema;
export const permissions = definePermissions({
  rooms: { read: (room) => isMember("rooms", room.id) },
  messages: { read: (message) => isMember("rooms", message.roomId) },
});
`;
}

describe("later versions of an encrypted app", () => {
  async function firstVersion(): Promise<string> {
    const root = await project(chat());
    await createAppMigration({ cwd: root, name: "init" });
    await generated(root);
    return root;
  }

  /** Write the second version's schema.ts, create its migration (asking nothing) and generate. */
  async function secondVersion(root: string, schema: string) {
    await writeFile(join(root, "schema.ts"), schema);
    const created = await createAppMigration({ cwd: root, name: "second" });
    expect(created.changed).toBe(true);
    expect(created.todos).toEqual([]);
    return generated(root);
  }

  const TEXT_ENTRY = { space: "roomId", scope: "rooms", columns: { text: "string" } };

  it("keeps the first version's declarations beside its snapshot, and in the history", async () => {
    const root = await firstVersion();
    const snapshots = await readdir(join(root, "migrations", "snapshots"));
    const sidecar = snapshots.find((file) => file.endsWith(".encryption.json"))!;
    expect(snapshots.sort()).toEqual(
      [sidecar, sidecar.replace(".encryption.json", ".json")].sort(),
    );
    expect(
      JSON.parse(await readFile(join(root, "migrations", "snapshots", sidecar), "utf8")),
    ).toEqual({ tables: { messages: TEXT_ENTRY } });

    const files = await secondVersion(root, chat({ rooms: ", topic: s.string().optional()" }));
    const app = JSON.parse(files.app) as {
      history: Record<string, { tables: Record<string, { encryption?: unknown }> }>;
    };
    expect(
      Object.values(app.history).map((version) => version.tables.messages!.encryption),
    ).toEqual([TEXT_ENTRY]);
  });

  it("generates a second version that adds columns to the encrypted table", async () => {
    const root = await firstVersion();
    // A new encrypted column is stored as required bytes: the stub fills the
    // rows stored before it with the empty ciphertext, read as never written.
    const files = await secondVersion(
      root,
      chat({
        messages:
          ", editedAt: s.timestamp().optional(), subject: s.string().optional().encrypted()",
      }),
    );
    const [stub] = (await readdir(join(root, "migrations"))).filter((file) => file.endsWith(".ts"));
    expect(await readFile(join(root, "migrations", stub!), "utf8")).toContain(
      '"subject": m.add.bytes({ default: new Uint8Array() }),',
    );
    const app = JSON.parse(files.app) as {
      tables: Record<string, { encryption?: unknown }>;
      history: Record<string, { tables: Record<string, { encryption?: unknown }> }>;
    };
    expect(app.tables.messages!.encryption).toEqual({
      ...TEXT_ENTRY,
      columns: { subject: "string?", text: "string" },
    });
    expect(
      Object.values(app.history).map((version) => version.tables.messages!.encryption),
    ).toEqual([TEXT_ENTRY]);
  });

  it("refuses a new equality column on an existing table, writing no stub", async () => {
    const root = await firstVersion();
    // The rows stored before it would hold no equality token, and writing
    // theirs (a backfill) needs their spaces' keys: refused before a stub
    // with a TODO the author couldn't resolve.
    await writeFile(
      join(root, "schema.ts"),
      chat({ messages: ", subject: s.string().optional().encrypted({ equality: true })" }),
    );
    await expect(createAppMigration({ cwd: root, name: "second" })).rejects.toThrow(
      /messages\.subject: finding rows by an encrypted column's value \(\.encrypted\(\{ equality: true \}\)\) on a table that already has rows isn't supported yet/,
    );
    expect(
      (await readdir(join(root, "migrations"))).filter((file) => file.endsWith(".ts")),
    ).toEqual([]);
  });

  it("fills a new encrypted column with a declared default without asking", async () => {
    const root = await firstVersion();
    const files = await secondVersion(
      root,
      chat({ messages: ', subject: s.string().default("(no subject)").encrypted()' }),
    );
    expect(JSON.parse(files.app).tables.messages.encryption.columns.subject).toEqual({
      type: "string",
      default: "(no subject)",
    });
  });

  const REQUIRED =
    /messages\.subject: a new encrypted column on a table that already has rows in version [0-9a-f]{12} must be optional or declare a default\. Sealing a value for the rows stored before it needs their spaces' keys/;

  it("refuses a new required encrypted column without a default on an existing table", async () => {
    const root = await firstVersion();
    await writeFile(
      join(root, "schema.ts"),
      chat({ messages: ", subject: s.string().encrypted()" }),
    );
    await expect(createAppMigration({ cwd: root, name: "second" })).rejects.toThrow(REQUIRED);
    expect(
      (await readdir(join(root, "migrations"))).filter((file) => file.endsWith(".ts")),
    ).toEqual([]);
  });

  it("refuses at generate a migration that adds one, as an earlier jazz-tools could write", async () => {
    const root = await firstVersion();
    // A migration and snapshot for the column with a default, then the
    // default taken out of both the schema and the snapshot's declarations.
    await secondVersion(root, chat({ messages: ', subject: s.string().default("").encrypted()' }));
    await writeFile(
      join(root, "schema.ts"),
      chat({ messages: ", subject: s.string().encrypted()" }),
    );
    const dir = join(root, "migrations", "snapshots");
    let edited = 0;
    for (const file of await readdir(dir)) {
      if (!file.endsWith(".encryption.json")) continue;
      const entries = JSON.parse(await readFile(join(dir, file), "utf8"));
      if (!entries.tables.messages.columns.subject) continue;
      entries.tables.messages.columns.subject = "string";
      await writeFile(join(dir, file), JSON.stringify(entries));
      edited += 1;
    }
    expect(edited).toBe(1);
    await expect(generateApp({ cwd: root })).rejects.toThrow(REQUIRED);
  });

  const CONVERSION =
    /messages\.text: its plaintext type changed from "string" to "int"\. Changing an encrypted column's plaintext type is a conversion, which isn't supported yet/;

  it("refuses an encrypted column's new plaintext type, which stores the same bytes", async () => {
    const root = await firstVersion();
    await writeFile(join(root, "schema.ts"), chat({ text: "s.int().encrypted()" }));
    await expect(createAppMigration({ cwd: root, name: "retype" })).rejects.toThrow(CONVERSION);
    await expect(createAppMigration({ cwd: root, name: "retype" })).rejects.toThrow(
      /which stores the same columns/,
    );
    await expect(generateApp({ cwd: root })).rejects.toThrow(CONVERSION);
  });

  it("refuses a new plaintext type beside another change", async () => {
    const root = await firstVersion();
    await writeFile(
      join(root, "schema.ts"),
      chat({ text: "s.int().encrypted()", rooms: ", topic: s.string().optional()" }),
    );
    await expect(createAppMigration({ cwd: root, name: "retype" })).rejects.toThrow(CONVERSION);
  });

  it("names the version whose declarations are missing", async () => {
    const root = await firstVersion();
    const dir = join(root, "migrations", "snapshots");
    const sidecar = (await readdir(dir)).find((file) => file.endsWith(".encryption.json"))!;
    await rm(join(dir, sidecar));
    await writeFile(join(root, "schema.ts"), chat({ rooms: ", topic: s.string().optional()" }));
    await expect(createAppMigration({ cwd: root, name: "second" })).rejects.toThrow(
      new RegExp(
        `version [0-9a-f]{12} declares encryption .*migrations/snapshots/${sidecar.replace(".", "\\.")}, which holds its encryption declarations, is missing`,
      ),
    );
  });
});

describe("what deploy would refuse is refused at generate, naming the table", () => {
  async function refusal(tables: string): Promise<string> {
    const root = await project(`
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
${tables}
});
`);
    const error = await generateApp({ cwd: root }).then(
      () => undefined,
      (e: Error) => e,
    );
    expect(error, "generate succeeded").toBeInstanceOf(Error);
    expect(existsSync(join(root, "jazz.app.json"))).toBe(false);
    return error!.message;
  }

  const ROOMS = "rooms: s.table({ name: s.string() }),";

  it("a space that names no relation of the table", async () => {
    expect(
      await refusal(`${ROOMS}
messages: s.table(
  { roomId: s.uuid(), text: s.string().encrypted() },
  { room: s.rel("rooms", "roomId") },
  { space: "rom" },
),`),
    ).toMatch(/messages: space "rom" is not a relation of the table/);
  });

  it("a space that names a reverse relation", async () => {
    expect(
      await refusal(`
rooms: s.table(
  { name: s.string().encrypted() },
  { messages: s.reverse("messages", "room") },
  { space: "messages" },
),
messages: s.table({ roomId: s.uuid() }, { room: s.rel("rooms", "roomId") }),`),
    ).toMatch(/rooms: space "messages" is a reverse relation/);
  });

  it("a space through a uuid-array column", async () => {
    expect(
      await refusal(`${ROOMS}
messages: s.table(
  { roomIds: s.array(s.uuid()), text: s.string().encrypted() },
  { rooms: s.rel("rooms", "roomIds") },
  { space: "rooms" },
),`),
    ).toMatch(/messages: space "rooms" goes through roomIds, which is not a uuid column/);
  });

  it("encrypted columns without a space", async () => {
    expect(
      await refusal(`${ROOMS}
messages: s.table(
  { roomId: s.uuid(), text: s.string().encrypted() },
  { room: s.rel("rooms", "roomId") },
),`),
    ).toMatch(/messages: encrypted columns \(text\) need the table's space/);
  });

  it("a merge strategy on an encrypted column, declared before or after", async () => {
    for (const text of [
      's.string().merge("text").encrypted()',
      's.string().encrypted().merge("text")',
    ]) {
      expect(
        await refusal(`${ROOMS}
messages: s.table(
  { roomId: s.uuid(), text: ${text} },
  { room: s.rel("rooms", "roomId") },
  { space: "room" },
),`),
      ).toMatch(/messages\.text: an encrypted column takes no merge strategy/);
    }
  });
});

describe("encryption on column builders that don't store a whole column", () => {
  it.each([
    ["array element", "s.array(s.string().encrypted())", "encrypt the whole array column"],
    [
      "nested array element",
      "s.array(s.array(s.string().encrypted()).optional()).encrypted()",
      "encrypt the whole array column",
    ],
    [
      "array element with a default",
      's.array(s.string().encrypted().default("untitled"))',
      "encrypt the whole array column",
    ],
    [
      "array element referencing a table",
      's.array(s.uuid().references("notes").encrypted())',
      "encrypt the whole array column",
    ],
    [
      "transform view",
      's.string().transform("asTitle", s.string().encrypted())',
      "use a stored column without a transform",
    ],
    [
      "array element of a transform view",
      's.string().transform("asTitles", s.array(s.string().encrypted()))',
      "use a stored column without a transform",
    ],
    ["JSON schema", "s.json(s.string().encrypted())", "encrypt the whole JSON column"],
    [
      "property in a JSON schema",
      's.json({ type: "object", properties: { title: s.string().encrypted() } })',
      "encrypt the whole JSON column",
    ],
    ["computed field", 's.computed(s.string().encrypted(), ["title"])', "encrypt a stored column"],
    [
      "array element of a computed field",
      's.computed(s.array(s.string().encrypted()), ["title"])',
      "encrypt a stored column",
    ],
  ])("refuses encryption on a %s during generate", async (_case, expression, guidance) => {
    const root = await project(`
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  notes: s.table({ title: s.string(), details: ${expression} }, {}, { space: "id" }),
});
`);
    await expect(generateApp({ cwd: root })).rejects.toThrow(
      new RegExp(`notes\\.details:.*${guidance}`),
    );
    expect(existsSync(join(root, "jazz.app.json"))).toBe(false);
  });

  it("encrypts a whole array column and preserves its optional default", async () => {
    const root = await project(`
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  notes: s.table(
    { titles: s.array(s.string()).encrypted().optional().default(["untitled"]) },
    {},
    { space: "id" },
  ),
});
export const permissions = schema.definePermissions({
  notes: { read: (note) => schema.isMember("notes", note.id) },
});
`);
    const files = await generated(root);
    const table = JSON.parse(files.app).tables.notes;
    expect(table.columns.titles).toBe("bytes");
    expect(table.encryption.columns.titles).toEqual({
      array: "string",
      optional: true,
      default: ["untitled"],
    });
  });
});

describe("encryption declarations in migrations", () => {
  it.each([
    ["add array operation", "add", "m.add.array({ of: s.string().encrypted(), default: null })"],
    [
      "drop array operation",
      "drop",
      "m.drop.array({ of: s.array(s.string().encrypted()), backwardsDefault: null })",
    ],
    ["add JSON operation", "add", "m.add.json({ schema: s.string().encrypted(), default: null })"],
    [
      "drop JSON operation",
      "drop",
      "m.drop.json({ schema: { items: s.string().encrypted() }, backwardsDefault: null })",
    ],
    ["source witness", "drop", "s.array(s.array(s.string())).optional().encrypted()"],
    ["target witness", "add", "s.array(s.string().encrypted()).optional()"],
  ])(
    "refuses encryption on a migration %s during generate",
    async (where, direction, expression) => {
      const withoutArray = chat();
      const withLabels = chat({
        messages: where.includes("JSON")
          ? ", labels: s.json().optional()"
          : direction === "add"
            ? ", labels: s.array(s.string()).optional()"
            : ", labels: s.array(s.array(s.string())).optional()",
      });
      const root = await project(direction === "add" ? withoutArray : withLabels);
      const original = await generated(root);
      await writeFile(join(root, "schema.ts"), direction === "add" ? withLabels : withoutArray);
      const created = await createAppMigration({ cwd: root, name: direction });
      expect(created.todos).toEqual([]);
      const path = created.file!;
      const stub = await readFile(path, "utf8");
      const edited = where.endsWith("operation")
        ? stub.replace(new RegExp(`m\\.${direction}\\.(?:array|json)\\(.*\\)`), expression)
        : stub.replace(/"labels": s\.array\(.*\)\.optional\(\)/, `"labels": ${expression}`);
      expect(edited).not.toBe(stub);
      await writeFile(path, edited);

      await expect(generateApp({ cwd: root })).rejects.toThrow(
        /messages\.labels:.*encryption.*schema\.ts/,
      );
      expect(await readFile(join(root, "jazz.app.json"), "utf8")).toBe(original.app);
    },
  );
});
