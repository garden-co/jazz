import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { loadAppFiles } from "../dev/codegen-napi.js";
import { readableSchema } from "../schema-dsl/define-schema.js";
import { readablePermissions } from "./dsl.js";
import * as chatReact from "./__fixtures__/chat-react.js";
import * as bigLabel from "./__fixtures__/big-label.js";

const examples = resolve(__dirname, "../../../../examples");

function files(module: { schema: unknown; permissions: unknown }) {
  const app = { format: 1, ...readableSchema(module.schema)! };
  const permissions = readablePermissions(module.permissions)!;
  return { app: JSON.stringify(app), permissions: JSON.stringify(permissions) };
}

interface Loaded {
  current: string;
  permissions: Record<string, unknown>;
}

async function load(app: string, permissions: string): Promise<Loaded> {
  return (await loadAppFiles(app, permissions)) as Loaded;
}

function example(name: string) {
  return {
    app: readFileSync(resolve(examples, name, "jazz.app.json"), "utf8"),
    permissions: JSON.parse(readFileSync(resolve(examples, name, "jazz.permissions.json"), "utf8")),
  };
}

describe("acceptance: the reference renderings", () => {
  it("chat-react compiles to main's tables and permissions", async () => {
    const ours = files(chatReact);
    const main = example("chat-react");
    // The one agreed difference: chatMembers.insert factors `mine` out.
    main.permissions.tables.chatMembers.insert = {
      allOf: [
        "userId = session.user",
        {
          anyOf: [
            "exists chats(id = this.chatId, isPublic = true)",
            "exists chats(id = this.chatId, $createdBy.account = session.user)",
            {
              allOf: [
                "joinCode != null",
                "exists chats(id = this.chatId, joinCode = this.joinCode)",
              ],
            },
          ],
        },
      ],
    };
    const loaded = await load(ours.app, ours.permissions);
    const expected = await load(main.app, JSON.stringify(main.permissions));
    expect(loaded.current).toBe(expected.current);
    expect(loaded.permissions).toEqual(expected.permissions);
  });

  it("big-label compiles to main's tables and the agreed permissions", async () => {
    const ours = files(bigLabel);
    const loaded = await load(ours.app, ours.permissions);
    const main = example("big-label");
    // The agreed differences from main's file: denied operations are left
    // out rather than "false" (outside Better Auth's tables), an editor is
    // `role in [...]` rather than anyOf of two exists, and memberships check
    // the role list and the person.
    const tables = main.permissions.tables as Record<string, Record<string, unknown>>;
    const editorOf = (org: string) =>
      `exists memberships(organizationId = ${org}, userId = session.user, role in ['admin', 'editor'])`;
    const rewrite = (policy: unknown): unknown => {
      if (typeof policy === "string") return policy;
      if (Array.isArray(policy)) return policy.map(rewrite);
      const object = policy as Record<string, unknown>;
      const items = object.anyOf as unknown[] | undefined;
      if (
        items?.length === 2 &&
        typeof items[0] === "string" &&
        items[0].endsWith("role = 'admin')") &&
        items[1] === items[0].replace("role = 'admin'", "role = 'editor'")
      )
        return editorOf(/organizationId = ([^,]+),/.exec(items[0])![1]!);
      return Object.fromEntries(Object.entries(object).map(([k, v]) => [k, rewrite(v)]));
    };
    for (const [table, rules] of Object.entries(tables)) {
      for (const [op, policy] of Object.entries(rules)) {
        if (policy === "false" && !table.startsWith("better_auth_")) delete rules[op];
        else rules[op] = rewrite(policy);
      }
    }
    const admin =
      "exists memberships(organizationId = this.organizationId, userId = session.user, role = 'admin')";
    const person = "exists people(id = this.personId, userId = this.userId)";
    tables.memberships!.insert = { allOf: [admin, person, "role in ['editor', 'viewer']"] };
    tables.memberships!.update = {
      old: admin,
      new: { allOf: [admin, person, "role in ['admin', 'editor', 'viewer']"] },
    };
    const expected = await load(main.app, JSON.stringify(main.permissions));
    expect(loaded.current).toBe(expected.current);
    expect(loaded.permissions).toEqual(expected.permissions);
  });

  it("prints the readable tree", () => {
    const tree = readablePermissions(chatReact.permissions)!;
    expect(tree.tables.chats).toEqual({
      read: {
        anyOf: [
          "isPublic = true",
          "exists chatMembers(chatId = this.id, userId = session.user)",
          "joinCode = session.claims.join_code",
        ],
      },
      insert: "$createdBy.account = session.user",
      update: {
        old: "exists chatMembers(chatId = this.id, userId = session.user)",
        new: {
          allOf: [
            "exists chatMembers(chatId = this.id, userId = session.user)",
            "exists chats(id = this.id, isPublic = this.isPublic)",
          ],
        },
      },
    });
    expect(readablePermissions(bigLabel.permissions)!.tables).toMatchSnapshot();
  });

  it("compiles one object's `old` comparisons to one self-lookup, beside its other keys", async () => {
    const { schema } = chatReact;
    const permissions = {
      ...Object.fromEntries(Object.keys(schema.tables).map((t) => [t, {}])),
      chats: {
        updateIf: true,
        updateTo: (chat: any, old: any) =>
          schema.allOf(schema.exists.chatMembers({ chatId: chat.id }), {
            isPublic: old.isPublic,
            name: schema.ne(null),
            joinCode: old.joinCode,
          }),
      },
    } as never;
    const tree = readablePermissions(schema.definePermissions(permissions))!;
    expect(tree.tables.chats!.update).toEqual({
      old: "true",
      new: {
        allOf: [
          "exists chatMembers(chatId = this.id)",
          {
            allOf: [
              "exists chats(id = this.id, isPublic = this.isPublic, joinCode = this.joinCode)",
              "name != null",
            ],
          },
        ],
      },
    });
    const app = JSON.stringify({
      $schema: "https://jazz.tools/schemas/jazz.app.v1.json",
      ...readableSchema(schema)!,
    });
    await expect(load(app, JSON.stringify(tree))).resolves.toBeTruthy();
  });
});
