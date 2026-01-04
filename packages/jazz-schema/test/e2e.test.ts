/**
 * End-to-end tests for @jazz/schema
 *
 * These tests use generated types from test/generated/ which are
 * generated from test/fixtures/notes-app.sql before tests run.
 *
 * Run: pnpm test
 */

import { describe, it, expect } from "vitest";
import { buildQuery, buildQueryById } from "../src/runtime.js";
import { schemaMeta, userMeta, folderMeta, noteMeta, tagMeta } from "./generated/meta.js";
import type {
  User,
  UserLoaded,
  UserIncludes,
  UserFilter,
  UserInsert,
  Folder,
  FolderLoaded,
  FolderIncludes,
  FolderFilter,
  FolderInsert,
  Note,
  NoteLoaded,
  NoteIncludes,
  NoteFilter,
  NoteInsert,
  Tag,
  TagLoaded,
  TagIncludes,
  TagFilter,
  TagInsert,
  ObjectId,
} from "./generated/types.js";

// === Metadata Tests ===

describe("Generated metadata", () => {
  it("exports schema with all tables", () => {
    expect(schemaMeta.tables).toHaveProperty("Users");
    expect(schemaMeta.tables).toHaveProperty("Folders");
    expect(schemaMeta.tables).toHaveProperty("Notes");
    expect(schemaMeta.tables).toHaveProperty("Tags");
  });

  describe("Users table metadata", () => {
    it("has correct name", () => {
      expect(userMeta.name).toBe("Users");
    });

    it("has all columns with correct types", () => {
      const colMap = Object.fromEntries(userMeta.columns.map((c) => [c.name, c]));

      expect(colMap.name).toEqual({ name: "name", type: { kind: "string" }, nullable: false });
      expect(colMap.email).toEqual({ name: "email", type: { kind: "string" }, nullable: false });
      expect(colMap.avatar).toEqual({ name: "avatar", type: { kind: "string" }, nullable: true });
      expect(colMap.age).toEqual({ name: "age", type: { kind: "i64" }, nullable: false });
      expect(colMap.score).toEqual({ name: "score", type: { kind: "f64" }, nullable: false });
      expect(colMap.isAdmin).toEqual({ name: "isAdmin", type: { kind: "bool" }, nullable: false });
    });

    it("has no forward refs", () => {
      expect(userMeta.refs).toHaveLength(0);
    });

    it("has reverse refs from Folders and Notes", () => {
      expect(userMeta.reverseRefs).toHaveLength(2);
      expect(userMeta.reverseRefs).toContainEqual({
        name: "Folders",
        sourceTable: "Folders",
        sourceColumn: "owner",
      });
      expect(userMeta.reverseRefs).toContainEqual({
        name: "Notes",
        sourceTable: "Notes",
        sourceColumn: "author",
      });
    });
  });

  describe("Folders table metadata", () => {
    it("has correct columns including refs", () => {
      const colMap = Object.fromEntries(folderMeta.columns.map((c) => [c.name, c]));

      expect(colMap.name).toEqual({ name: "name", type: { kind: "string" }, nullable: false });
      expect(colMap.owner).toEqual({
        name: "owner",
        type: { kind: "ref", table: "Users" },
        nullable: false,
      });
      expect(colMap.parent).toEqual({
        name: "parent",
        type: { kind: "ref", table: "Folders" },
        nullable: true,
      });
    });

    it("has forward refs to Users and self", () => {
      expect(folderMeta.refs).toHaveLength(2);
      expect(folderMeta.refs).toContainEqual({
        column: "owner",
        targetTable: "Users",
        nullable: false,
      });
      expect(folderMeta.refs).toContainEqual({
        column: "parent",
        targetTable: "Folders",
        nullable: true,
      });
    });

    it("has reverse refs from Folders and Notes", () => {
      expect(folderMeta.reverseRefs).toHaveLength(2);
      expect(folderMeta.reverseRefs).toContainEqual({
        name: "Folders",
        sourceTable: "Folders",
        sourceColumn: "parent",
      });
      expect(folderMeta.reverseRefs).toContainEqual({
        name: "Notes",
        sourceTable: "Notes",
        sourceColumn: "folder",
      });
    });
  });

  describe("Notes table metadata", () => {
    it("has all columns", () => {
      expect(noteMeta.columns).toHaveLength(7);
      const names = noteMeta.columns.map((c) => c.name);
      expect(names).toContain("title");
      expect(names).toContain("content");
      expect(names).toContain("author");
      expect(names).toContain("folder");
      expect(names).toContain("createdAt");
      expect(names).toContain("updatedAt");
      expect(names).toContain("isPublic");
    });

    it("has forward refs to Users and Folders", () => {
      expect(noteMeta.refs).toHaveLength(2);
      expect(noteMeta.refs).toContainEqual({
        column: "author",
        targetTable: "Users",
        nullable: false,
      });
      expect(noteMeta.refs).toContainEqual({
        column: "folder",
        targetTable: "Folders",
        nullable: true,
      });
    });

    it("has no reverse refs", () => {
      expect(noteMeta.reverseRefs).toHaveLength(0);
    });
  });

  describe("Tags table metadata", () => {
    it("is a simple table with no refs", () => {
      expect(tagMeta.refs).toHaveLength(0);
      expect(tagMeta.reverseRefs).toHaveLength(0);
      expect(tagMeta.columns).toHaveLength(2);
    });
  });
});

// === Query Builder Tests ===

describe("buildQuery", () => {
  describe("basic queries", () => {
    it("builds simple SELECT *", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {});
      expect(sql).toBe("SELECT n.* FROM Notes n");
    });

    it("uses first letter of table as alias", () => {
      const userSql = buildQuery(userMeta, schemaMeta, {});
      expect(userSql).toBe("SELECT u.* FROM Users u");

      const folderSql = buildQuery(folderMeta, schemaMeta, {});
      expect(folderSql).toBe("SELECT f.* FROM Folders f");
    });
  });

  describe("where clauses", () => {
    it("builds equals filter with direct value", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { title: "Hello" } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.title = 'Hello'");
    });

    it("builds equals filter with filter object", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { title: { equals: "Hello" } } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.title = 'Hello'");
    });

    it("builds not filter", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { title: { not: "Draft" } } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.title != 'Draft'");
    });

    it("builds null checks", () => {
      const nullSql = buildQuery(noteMeta, schemaMeta, {
        where: { folder: null } as NoteFilter,
      });
      expect(nullSql).toBe("SELECT n.* FROM Notes n WHERE n.folder IS NULL");

      const notNullSql = buildQuery(noteMeta, schemaMeta, {
        where: { folder: { not: null } } as NoteFilter,
      });
      expect(notNullSql).toBe("SELECT n.* FROM Notes n WHERE n.folder IS NOT NULL");
    });

    it("builds comparison filters for bigint", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { createdAt: { gte: BigInt(1000), lt: BigInt(2000) } } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.createdAt >= 1000 AND n.createdAt < 2000");
    });

    it("builds comparison filters for numbers", () => {
      const sql = buildQuery(userMeta, schemaMeta, {
        where: { score: { gt: 90.5 } } as UserFilter,
      });
      expect(sql).toBe("SELECT u.* FROM Users u WHERE u.score > 90.5");
    });

    it("builds string contains filter", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { title: { contains: "test" } } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.title LIKE '%test%'");
    });

    it("builds string startsWith filter", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { title: { startsWith: "Draft:" } } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.title LIKE 'Draft:%'");
    });

    it("builds string endsWith filter", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { title: { endsWith: ".md" } } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.title LIKE '%.md'");
    });

    it("builds IN filter", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { title: { in: ["A", "B", "C"] } } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.title IN ('A', 'B', 'C')");
    });

    it("builds NOT IN filter", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { title: { notIn: ["Draft", "Deleted"] } } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.title NOT IN ('Draft', 'Deleted')");
    });

    it("builds boolean filter", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { isPublic: true } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.isPublic = TRUE");
    });

    it("escapes single quotes in strings", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { title: "It's a test" } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.title = 'It''s a test'");
    });
  });

  describe("combinators", () => {
    it("builds AND combinator", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: {
          AND: [{ isPublic: true }, { title: { startsWith: "Published" } }],
        } as NoteFilter,
      });
      expect(sql).toBe(
        "SELECT n.* FROM Notes n WHERE (n.isPublic = TRUE AND n.title LIKE 'Published%')"
      );
    });

    it("builds OR combinator", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: {
          OR: [{ title: "A" }, { title: "B" }],
        } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE (n.title = 'A' OR n.title = 'B')");
    });

    it("builds NOT combinator", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: {
          NOT: { isPublic: false },
        } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE NOT (n.isPublic = FALSE)");
    });

    it("builds nested combinators", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: {
          AND: [{ OR: [{ title: "A" }, { title: "B" }] }, { isPublic: true }],
        } as NoteFilter,
      });
      expect(sql).toBe(
        "SELECT n.* FROM Notes n WHERE ((n.title = 'A' OR n.title = 'B') AND n.isPublic = TRUE)"
      );
    });

    it("combines top-level conditions with AND", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { title: "Test", isPublic: true } as NoteFilter,
      });
      expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.title = 'Test' AND n.isPublic = TRUE");
    });
  });

  describe("include (forward refs)", () => {
    it("includes forward ref with JOIN", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        include: { author: true },
      });
      expect(sql).toContain("JOIN Users author ON n.author = author.id");
      expect(sql).toContain("ROW(author.id,");
      expect(sql).toContain("author.name");
      expect(sql).toContain(") as author");
    });

    it("uses LEFT JOIN for nullable refs", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        include: { folder: true },
      });
      expect(sql).toContain("LEFT JOIN Folders folder ON n.folder = folder.id");
    });

    it("includes multiple forward refs", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        include: { author: true, folder: true },
      });
      expect(sql).toContain("JOIN Users author");
      expect(sql).toContain("LEFT JOIN Folders folder");
    });
  });

  describe("include (reverse refs)", () => {
    it("includes reverse ref with ARRAY subquery", () => {
      const sql = buildQuery(userMeta, schemaMeta, {
        include: { Notes: true },
      });
      expect(sql).toContain(
        "ARRAY(SELECT n_inner FROM Notes n_inner WHERE n_inner.author = u.id) as Notes"
      );
    });

    it("includes multiple reverse refs", () => {
      const sql = buildQuery(userMeta, schemaMeta, {
        include: { Notes: true, Folders: true },
      });
      expect(sql).toContain("ARRAY(SELECT n_inner FROM Notes n_inner WHERE n_inner.author = u.id)");
      expect(sql).toContain(
        "ARRAY(SELECT f_inner FROM Folders f_inner WHERE f_inner.owner = u.id)"
      );
    });
  });

  describe("combined where and include", () => {
    it("builds query with both where and include", () => {
      const sql = buildQuery(noteMeta, schemaMeta, {
        where: { isPublic: true } as NoteFilter,
        include: { author: true },
      });
      expect(sql).toContain("SELECT");
      expect(sql).toContain("ROW(author.id,");
      expect(sql).toContain("FROM Notes n");
      expect(sql).toContain("JOIN Users author");
      expect(sql).toContain("WHERE n.isPublic = TRUE");
    });
  });
});

describe("buildQueryById", () => {
  it("builds query with id filter", () => {
    const sql = buildQueryById(noteMeta, schemaMeta, "abc123");
    expect(sql).toBe("SELECT n.* FROM Notes n WHERE n.id = 'abc123'");
  });

  it("includes relations in by-id query", () => {
    const sql = buildQueryById(noteMeta, schemaMeta, "abc123", {
      include: { author: true },
    });
    expect(sql).toContain("JOIN Users author ON n.author = author.id");
    expect(sql).toContain("WHERE n.id = 'abc123'");
  });
});

// === Type Correctness Tests ===
// These tests verify that the generated types work correctly at compile time.
// If these compile, the types are correct.

describe("Type correctness", () => {
  describe("Row types", () => {
    it("User has correct fields", () => {
      const user: User = {
        id: "test-id",
        name: "Alice",
        email: "alice@example.com",
        avatar: null,
        age: BigInt(30),
        score: 95.5,
        isAdmin: false,
      };
      expect(user.name).toBe("Alice");
      expect(user.avatar).toBeNull();
    });

    it("Note has ref columns as ObjectId", () => {
      const note: Note = {
        id: "note-id",
        title: "Test",
        content: "Content",
        author: "user-id", // ObjectId, not User
        folder: null,
        createdAt: BigInt(Date.now()),
        updatedAt: BigInt(Date.now()),
        isPublic: true,
      };
      expect(note.author).toBe("user-id");
    });
  });

  describe("Insert types", () => {
    it("UserInsert omits id and allows optional avatar", () => {
      const insert: UserInsert = {
        name: "Bob",
        email: "bob@example.com",
        // avatar is optional
        age: BigInt(25),
        score: 80.0,
        isAdmin: true,
      };
      expect(insert.name).toBe("Bob");
    });

    it("FolderInsert accepts ObjectId or object for refs", () => {
      const user: User = {
        id: "user-id",
        name: "Alice",
        email: "alice@example.com",
        avatar: null,
        age: BigInt(30),
        score: 95.5,
        isAdmin: false,
      };

      // Can use ObjectId
      const insert1: FolderInsert = {
        name: "Folder",
        owner: "user-id",
      };

      // Can use User object
      const insert2: FolderInsert = {
        name: "Folder",
        owner: user,
      };

      expect(insert1.owner).toBe("user-id");
      expect(insert2.owner).toBe(user);
    });
  });

  describe("Loaded types with includes", () => {
    it("UserLoaded without includes has no relations", () => {
      const user: UserLoaded<{}> = {
        id: "user-id",
        name: "Alice",
        email: "alice@example.com",
        avatar: null,
        age: BigInt(30),
        score: 95.5,
        isAdmin: false,
      };
      // @ts-expect-error - Notes should not exist without includes
      const _notes = user.Notes;
      expect(user.name).toBe("Alice");
    });

    it("UserLoaded with Notes includes Notes array", () => {
      const user: UserLoaded<{ Notes: true }> = {
        id: "user-id",
        name: "Alice",
        email: "alice@example.com",
        avatar: null,
        age: BigInt(30),
        score: 95.5,
        isAdmin: false,
        Notes: [
          {
            id: "note-id",
            title: "Test",
            content: "Content",
            author: "user-id",
            folder: null,
            createdAt: BigInt(0),
            updatedAt: BigInt(0),
            isPublic: true,
          },
        ],
      };
      expect(user.Notes).toHaveLength(1);
      expect(user.Notes[0].title).toBe("Test");
    });

    it("NoteLoaded with author includes resolves author to User", () => {
      const note: NoteLoaded<{ author: true }> = {
        id: "note-id",
        title: "Test",
        content: "Content",
        author: {
          id: "user-id",
          name: "Alice",
          email: "alice@example.com",
          avatar: null,
          age: BigInt(30),
          score: 95.5,
          isAdmin: false,
        },
        folder: null, // still ObjectId | null since folder not included
        createdAt: BigInt(0),
        updatedAt: BigInt(0),
        isPublic: true,
      };
      expect(note.author.name).toBe("Alice");
    });

    it("FolderLoaded with nested includes", () => {
      const folder: FolderLoaded<{ owner: { Notes: true } }> = {
        id: "folder-id",
        name: "My Folder",
        owner: {
          id: "user-id",
          name: "Alice",
          email: "alice@example.com",
          avatar: null,
          age: BigInt(30),
          score: 95.5,
          isAdmin: false,
          Notes: [],
        },
        parent: null,
      };
      expect(folder.owner.name).toBe("Alice");
      expect(folder.owner.Notes).toEqual([]);
    });
  });

  describe("Filter types", () => {
    it("UserFilter accepts string filters for string columns", () => {
      const where: UserFilter = {
        name: { contains: "Alice" },
        email: { endsWith: "@example.com" },
      };
      expect(where.name).toEqual({ contains: "Alice" });
    });

    it("UserFilter accepts BigIntFilter for i64 columns", () => {
      const where: UserFilter = {
        age: { gte: BigInt(18), lt: BigInt(100) },
      };
      expect(where.age).toEqual({ gte: BigInt(18), lt: BigInt(100) });
    });

    it("UserFilter accepts NumberFilter for f64 columns", () => {
      const where: UserFilter = {
        score: { gt: 50.0 },
      };
      expect(where.score).toEqual({ gt: 50.0 });
    });

    it("UserFilter accepts BoolFilter for bool columns", () => {
      const where: UserFilter = {
        isAdmin: { equals: true },
      };
      expect(where.isAdmin).toEqual({ equals: true });
    });

    it("NoteFilter accepts null for nullable columns", () => {
      const where: NoteFilter = {
        folder: null,
      };
      expect(where.folder).toBeNull();
    });

    it("Filter accepts combinators", () => {
      const where: NoteFilter = {
        OR: [
          { isPublic: true },
          { author: "special-user-id" },
        ],
        NOT: { title: { startsWith: "Draft" } },
      };
      expect(where.OR).toHaveLength(2);
    });
  });

  describe("Includes types", () => {
    it("NoteIncludes only allows valid relations", () => {
      const includes: NoteIncludes = {
        author: true,
        folder: { owner: true },
      };
      // @ts-expect-error - invalid should not be allowed
      const _invalid: NoteIncludes = { invalid: true };
      expect(includes.author).toBe(true);
    });

    it("FolderIncludes allows self-referential includes", () => {
      const includes: FolderIncludes = {
        parent: {
          parent: {
            parent: true,
          },
        },
      };
      expect(includes.parent).toEqual({ parent: { parent: true } });
    });
  });
});
