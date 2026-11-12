import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";

const app = s.defineApp(
  {
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
  },
  { migrations: "none" },
);

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

  it("resolves exact whitespace-containing forward and reverse names in hopTo and allowedTo", () => {
    const compile = (forward: string, reverse: string) => {
      const exact = s.defineApp(
        {
          roots: s.table({}, { [reverse]: s.reverse("leaves", forward) }),
          leaves: s.table({ rootId: s.uuid() }, { [forward]: s.rel("roots", "rootId") }),
        },
        { migrations: "none" },
      );
      return s.definePermissions(exact, ({ policy, allowedTo }) => {
        policy.leaves.allowRead.where(policy.exists(policy.leaves.hopTo(forward)));
        policy.roots.allowRead.where(policy.exists(policy.roots.hopTo(reverse)));
        policy.leaves.allowInsert.where(allowedTo.insert(forward));
        policy.roots.allowInsert.where(allowedTo.insert(reverse));
      });
    };
    expect(compile(" linked ", " incoming ")).toEqual(compile("linked", "incoming"));
  });

  it("rejects empty and undeclared padded hop names without normalizing them", () => {
    for (const name of ["", " writer "]) {
      expect(() =>
        s.definePermissions(app, ({ policy }) => {
          policy.posts.allowRead.where(policy.exists(policy.posts.hopTo(name)));
        }),
      ).toThrow(name === "" ? /non-empty relation name/ : /Unknown relation/);
    }
  });

  it("resolves reusable names per rule table without mutating the expression", () => {
    const reused = s.defineApp(
      {
        roots: s.table({}, { linked: s.reverse("leaves", "linked") }),
        leaves: s.table({ rootId: s.uuid() }, { linked: s.rel("roots", "rootId") }),
      },
      { migrations: "none" },
    );
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

describe("relation-qualified where(...) keys", () => {
  const twoWays = s.defineApp(
    {
      users: s.table({ uid: s.string() }, {}),
      docs: s.table(
        { a: s.uuid(), b: s.uuid() },
        { ua: s.rel("users", "a"), ub: s.rel("users", "b") },
      ),
    },
    { migrations: "none" },
  );
  const oneWay = s.defineApp(
    {
      users: s.table({ uid: s.string() }, {}),
      docs: s.table({ a: s.uuid(), b: s.uuid() }, { ua: s.rel("users", "a") }),
    },
    { migrations: "none" },
  );
  const readWhere = (app: typeof twoWays | typeof oneWay, key: string) =>
    s.definePermissions(app, ({ policy, session }) => {
      policy.docs.allowRead.where({ [key]: session.user });
    }).docs?.select?.using;

  it("resolve a column through the named relation, as the table name does when only one relation leads there", () => {
    expect(readWhere(twoWays, "ua.uid")).toEqual(readWhere(oneWay, "users.uid"));
    expect(readWhere(oneWay, "ua.uid")).toEqual(readWhere(oneWay, "users.uid"));
    expect(readWhere(twoWays, "ub.uid")).not.toEqual(readWhere(twoWays, "ua.uid"));
  });

  it("name the relations to qualify with when the table is ambiguous", () => {
    expect(() => readWhere(twoWays, "users.uid")).toThrow(
      /table "users" is ambiguous from "docs": relations "ua" \(to "users"\), "ub" \(to "users"\) all lead there\. Qualify the column with a relation's name instead \("ua\.uid" or "ub\.uid"\)/,
    );
  });

  it("list the table's relations when the qualifier is neither a relation nor a reachable table", () => {
    expect(() => readWhere(twoWays, "doctor.uid")).toThrow(
      /"doctor" is neither a relation of "docs" nor a table that one of its relations leads to\. Its relations are "ua" \(to "users"\), "ub" \(to "users"\)\./,
    );
  });

  it("resolve a relation named after its table as the table, as before relation names qualified keys", () => {
    // Users name a reverse relation after its table, with a sibling to the
    // same table: the table-qualified key keeps resolving by its column.
    const selfNamed = s.defineApp(
      {
        users: s.table(
          { uid: s.string() },
          { todos: s.reverse("todos", "owner"), assigned: s.reverse("todos", "assignee") },
        ),
        todos: s.table(
          { ownerId: s.uuid(), assigneeId: s.uuid(), title: s.string() },
          { owner: s.rel("users", "ownerId"), assignee: s.rel("users", "assigneeId") },
        ),
      },
      { migrations: "none" },
    );
    const usersWhere = (key: string) =>
      s.definePermissions(selfNamed, ({ policy }) => {
        policy.users.allowRead.where({ [key]: "x" });
      }).users?.select?.using;
    expect(usersWhere("todos.assigneeId")).toEqual(usersWhere("assigned.assigneeId"));
    expect(usersWhere("todos.ownerId")).not.toEqual(usersWhere("todos.assigneeId"));

    // With no sibling, a self-named relation compiles exactly as a
    // differently named one does under its table's name.
    const single = (name: string) =>
      s.defineApp(
        {
          users: s.table({ uid: s.string() }, {}),
          docs: s.table({ a: s.uuid() }, { [name]: s.rel("users", "a") }),
        },
        { migrations: "none" },
      );
    const docsWhere = (app: ReturnType<typeof single>, key: string) =>
      s.definePermissions(app, ({ policy }) => {
        policy.docs.allowRead.where({ [key]: "x" });
      }).docs?.select?.using;
    expect(docsWhere(single("users"), "users.uid")).toEqual(
      docsWhere(single("author"), "users.uid"),
    );
  });

  it("refuse a qualifier that names a relation to one table and a table other relations lead to", () => {
    const clash = s.defineApp(
      {
        users: s.table({ uid: s.string() }, {}),
        orgs: s.table({ name: s.string() }, {}),
        docs: s.table(
          { a: s.uuid(), o: s.uuid() },
          { orgs: s.rel("users", "a"), org: s.rel("orgs", "o") },
        ),
      },
      { migrations: "none" },
    );
    expect(() =>
      s.definePermissions(clash, ({ policy }) => {
        policy.docs.allowRead.where({ "orgs.name": "x" });
      }),
    ).toThrow(
      /"orgs" names both the relation "orgs" \(to "users"\) and the table "orgs", which relation "org" leads to\. Rename the relation/,
    );
  });
});
