import { describe, expect, it } from "vitest";
import * as s from "../schema-builders.js";
import { loadAppFiles } from "../dev/codegen-napi.js";
import { readableSchema } from "../schema-dsl/define-schema.js";
import { readablePermissions } from "./dsl.js";

const schema = s.defineSchema({
  rooms: s.table({ name: s.string(), ownerId: s.uuid() }, {}, { space: "id" }),
  messages: s.table(
    { roomId: s.uuid(), authorId: s.uuid(), text: s.string().encrypted() },
    { room: s.rel("rooms", "roomId") },
    { space: "room" },
  ),
  profiles: s.table({ roomId: s.uuid(), accountId: s.uuid() }, { room: s.rel("rooms", "roomId") }),
});
const { definePermissions, isMember, exists, session } = schema;

const MEMBERS_OF_ROOM =
  "exists __e2ee_members(scopeId = table_id('rooms'), identifier = this.id, accountId = session.user)";
const MEMBERS_OF_MESSAGE_ROOM =
  "exists __e2ee_members(scopeId = table_id('rooms'), identifier = this.roomId, accountId = session.user)";

/** The permissions with `messages.read` built by `rule`, the space table's read rule its members rule. */
function messagesRead(rule: (message: any) => unknown) {
  return definePermissions({
    rooms: { read: (room) => isMember("rooms", room.id) },
    messages: { read: rule as never },
    profiles: {},
  });
}

function refusal(build: () => unknown): string {
  try {
    build();
  } catch (error) {
    return (error as Error).message;
  }
  throw new Error("expected a refusal");
}

describe("isMember", () => {
  it("renders exactly the members rule, through the space table's id or a row's space column", () => {
    const permissions = messagesRead((message) => isMember("rooms", message.roomId));
    expect(readablePermissions(permissions)!.tables).toEqual({
      rooms: { read: MEMBERS_OF_ROOM },
      messages: { read: MEMBERS_OF_MESSAGE_ROOM },
      profiles: {},
    });
  });

  it("combines like any policy", () => {
    const permissions = messagesRead((message) =>
      schema.allOf(isMember("rooms", message.roomId), { authorId: session.user }),
    );
    expect(readablePermissions(permissions)!.tables.messages).toEqual({
      read: { allOf: [MEMBERS_OF_MESSAGE_ROOM, "authorId = session.user"] },
    });
  });

  it("gives files deploy loads: the space table's read rule is its members rule", async () => {
    const permissions = messagesRead((message) => isMember("rooms", message.roomId));
    const app = JSON.stringify({
      $schema: "https://jazz.tools/schemas/jazz.app.v1.json",
      ...readableSchema(schema),
    });
    const loaded = (await loadAppFiles(app, JSON.stringify(readablePermissions(permissions)))) as {
      permissions: Record<string, unknown>;
    };
    expect(Object.keys(loaded.permissions).sort()).toEqual(["messages", "rooms"]);
    // A near miss of the same rule, written by hand, is what deploy refuses.
    const nearMiss = {
      ...readablePermissions(permissions)!,
      tables: {
        ...readablePermissions(permissions)!.tables,
        rooms: {
          read: MEMBERS_OF_ROOM.replace("accountId = session.user", "accountId = this.ownerId"),
        },
      },
    };
    await expect(loadAppFiles(app, JSON.stringify(nearMiss))).rejects.toThrow(/rooms/);
  });

  it("refuses a table that is not a space table", () => {
    expect(
      refusal(() => messagesRead((message) => isMember("messages" as never, message.id))),
    ).toMatch(
      /^jazz\.permissions\.json: tables\.messages\.read: isMember\("messages", …\): messages is not a space table of this schema/,
    );
  });

  it("refuses an id that is not a column of the checked row leading to the space table", () => {
    // A column with no relation to rooms, and the row's own id on another table.
    for (const id of [(m: any) => m.authorId, (m: any) => m.id]) {
      expect(refusal(() => messagesRead((message) => isMember("rooms", id(message))))).toMatch(
        /tables\.messages\.read: isMember\("rooms", …\): messages\.(authorId|id) doesn't lead to rooms/,
      );
    }
    // A session value or a literal is not a row's column.
    for (const id of [session.user, "6b1a4c1e-0000-4000-8000-000000000000"]) {
      expect(refusal(() => messagesRead(() => isMember("rooms", id as never)))).toMatch(
        /tables\.messages\.read: isMember\("rooms", …\): the id is the checked row's/,
      );
    }
  });

  it("refuses a column of a row other than the checked one", () => {
    expect(
      refusal(() =>
        definePermissions({
          rooms: { read: (room) => isMember("rooms", room.id) },
          messages: {},
          profiles: {
            read: () => exists.messages((message: any) => isMember("rooms", message.roomId)),
          },
        }),
      ),
    ).toMatch(/tables\.profiles\.read: .*messages\.roomId belongs to an enclosing `exists`/);
  });

  it("is refused in a schema without spaces", () => {
    const plain = s.defineSchema({ rooms: s.table({ name: s.string() }) });
    expect(
      refusal(() =>
        plain.definePermissions({ rooms: { read: (room) => plain.isMember("rooms", room.id) } }),
      ),
    ).toMatch(/isMember\("rooms", …\): rooms is not a space table of this schema/);
  });
});

describe("types", () => {
  it("accept only a forward uuid relation or id as a space, and a schema table in isMember", () => {
    // Compiled, never run: the checks are the `@ts-expect-error`s.
    const unused = () => {
      s.table(
        { roomId: s.uuid() },
        { room: s.rel("rooms", "roomId") },
        // @ts-expect-error: not a relation of the table
        { space: "rom" },
      );
      s.table(
        { name: s.string() },
        { messages: s.reverse("messages", "room") },
        // @ts-expect-error: a reverse relation can't name a space
        { space: "messages" },
      );
      s.table(
        { roomIds: s.array(s.uuid()) },
        { rooms: s.rel("rooms", "roomIds") },
        // @ts-expect-error: a uuid array can't name one space
        { space: "rooms" },
      );
      definePermissions({
        // @ts-expect-error: isMember takes a table of the schema
        rooms: { read: (room) => isMember("nope", room.id) },
        messages: {},
        profiles: {},
      });
      definePermissions({
        // @ts-expect-error: the id is a uuid column
        rooms: { read: (room) => isMember("rooms", room.name) },
        messages: {},
        profiles: {},
      });
    };
    expect(typeof unused).toBe("function");
  });
});
