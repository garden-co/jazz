import { existsSync, readFileSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
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
