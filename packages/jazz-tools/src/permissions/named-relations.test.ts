import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";

const app = s.defineApp({
  users: s.table(
    { name: s.string() },
    { authored: s.reverse("posts", "writer"), saved: s.reverse("posts", "audience") },
  ),
  posts: s.table(
    { authorId: s.uuid().optional(), readerIds: s.array(s.uuid()), title: s.string() },
    {
      writer: s.rel("users", "authorId"),
      audience: s.rel("users", "readerIds"),
    },
  ),
});

describe("declared relationships in permissions", () => {
  it("preserves operation, columns, and both update row images for renamed forward and reverse names", () => {
    const policies = s.definePermissions(app, ({ policy, allowedTo }) => {
      const author = allowedTo.update("writer");
      policy.posts.allowRead.where(allowedTo.read("writer", { maxDepth: 0 }));
      policy.posts.allowInsert.where(allowedTo.insert("audience"));
      policy.posts.allowUpdate.whereOld(author).whereNew(author);
      policy.posts.allowDelete.where(allowedTo.delete("writer"));
      policy.users.allowRead.where(allowedTo.read("authored"));
      policy.users.allowInsert.where(allowedTo.insert("saved"));
      policy.users.allowUpdate.where(allowedTo.update("authored"));
      policy.users.allowDelete.where(allowedTo.delete("authored"));
    });
    expect(policies.posts?.select?.using).toEqual({
      type: "Inherits",
      operation: "Select",
      via_column: "authorId",
      max_depth: 0,
    });
    for (const [action, operation, phase] of [
      ["insert", "Insert", "with_check"],
      ["update", "Update", "using"],
      ["delete", "Delete", "using"],
    ] as const) {
      expect(policies.posts?.[action]?.[phase]).toEqual({
        type: "Inherits",
        operation,
        via_column: action === "insert" ? "readerIds" : "authorId",
      });
      expect(policies.users?.[action]?.[phase]).toEqual({
        type: "InheritsReferencing",
        operation,
        source_table: "posts",
        via_column: action === "insert" ? "readerIds" : "authorId",
      });
    }
    expect(policies.posts?.update?.with_check).toEqual(policies.posts?.update?.using);
    expect(policies.users?.select?.using).toEqual({
      type: "InheritsReferencing",
      operation: "Select",
      source_table: "posts",
      via_column: "authorId",
    });
  });

  it("resolves reusable names per rule table without mutating the expression", () => {
    const reused = s.defineApp({
      roots: s.table({}, { linked: s.reverse("leaves", "linked") }),
      leaves: s.table({ rootId: s.uuid() }, { linked: s.rel("roots", "rootId") }),
    });
    const policies = s.definePermissions(reused, ({ policy, allowedTo }) => {
      const grant = allowedTo.read("linked");
      policy.roots.allowRead.where(grant);
      policy.leaves.allowRead.where(grant);
    });
    expect(policies.roots?.select?.using?.type).toBe("InheritsReferencing");
    expect(policies.leaves?.select?.using?.type).toBe("Inherits");
  });

  it("rejects undeclared names, the wrong table, reverse source names, and unsupported bounds", () => {
    for (const name of ["authorId", "author", "postsViaAuthor", " writer", ""]) {
      expect(() =>
        s.definePermissions(app, ({ policy, allowedTo }) => {
          policy.posts.allowRead.where(allowedTo.read(name as "writer"));
        }),
      ).toThrow(/relation|relationship/i);
    }
    expect(() =>
      s.definePermissions(app, ({ policy, allowedTo }) => {
        policy.posts.allowRead.where(allowedTo.read("authored"));
      }),
    ).toThrow(/Unknown relation "authored" on table "posts"/);
    expect(() =>
      s.definePermissions(app, ({ policy, allowedTo }) => {
        policy.posts.allowRead.where(allowedTo.readReferencing(policy.users, "authored"));
      }),
    ).toThrow(/requires a declared forward relationship/);
    expect(() =>
      s.definePermissions(app, ({ policy, allowedTo }) => {
        policy.users.allowRead.where(allowedTo.read("authored", { maxDepth: 0 }));
      }),
    ).toThrow(/reverse relationships do not support maxDepth/);
  });
});
