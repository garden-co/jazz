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
              allOf: ["joinCode != null", "exists chats(id = this.chatId, joinCode = this.joinCode)"],
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
    const expected = await load(main.app, JSON.stringify(main.permissions));
    expect(loaded.current).toBe(expected.current);
    expect(Object.keys(loaded.permissions).sort()).toEqual(Object.keys(expected.permissions).sort());
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
});
