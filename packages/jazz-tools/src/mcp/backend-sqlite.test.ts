import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { createSqliteBackend } from "./backend-sqlite.js";

// ---------------------------------------------------------------------------
// Fixture DB
//
// Three pages reflecting jazz1 docs structure:
//   "quickstart"                          — installing + schema definition
//   "core-concepts/covalues"              — CoMap + CoList (mentions "CoValue" often)
//   "core-concepts/subscription-and-loading" — subscribing + deep loading (mentions "CoValue" too)
//
// ASCII map of expected related links:
//   covalues ←→ subscription-and-loading  (both heavily mention "CoValue")
//   quickstart is largely unrelated to the others
// ---------------------------------------------------------------------------

let tmpDir: string;
let dbPath: string;

beforeEach(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "backend-sqlite-test-"));
  dbPath = join(tmpDir, "docs-index.db");

  const db = new DatabaseSync(dbPath);

  db.exec(`
    CREATE TABLE pages (
      title       TEXT NOT NULL,
      slug        TEXT PRIMARY KEY,
      description TEXT NOT NULL,
      body        TEXT NOT NULL
    );
    CREATE VIRTUAL TABLE sections_fts USING fts5(
      title,
      slug        UNINDEXED,
      section_heading,
      body,
      tokenize    = 'unicode61'
    );
  `);

  const insertPage = db.prepare("INSERT INTO pages VALUES (?, ?, ?, ?)");
  const insertSection = db.prepare(
    "INSERT INTO sections_fts VALUES (?, ?, ?, ?)",
  );

  // Page 1: Quickstart
  insertPage.run(
    "Quickstart",
    "quickstart",
    "Get started building a simple front-end app with Jazz in 10 minutes.",
    "Build your first Jazz app. Install jazz-tools to get started. Run npm install jazz-tools to install the package.",
  );
  insertSection.run(
    "Quickstart",
    "quickstart",
    "",
    "Build your first Jazz app. The jazz-tools package includes everything you need.",
  );
  insertSection.run(
    "Quickstart",
    "quickstart",
    "Install Jazz",
    "Install jazz-tools to get started. Run npm install jazz-tools to install the package.",
  );
  insertSection.run(
    "Quickstart",
    "quickstart",
    "Set Up Your Schema",
    "Define a CoMap schema to describe your app's data structure using co.map().",
  );

  // Page 2: CoValues
  insertPage.run(
    "CoValues",
    "core-concepts/covalues",
    "CoValues are the collaborative data types at the core of Jazz.",
    "CoValues are Jazz's collaborative values. A CoValue can be a CoMap, CoList, or CoFeed. CoValues are reactive and sync automatically.",
  );
  insertSection.run(
    "CoValues",
    "core-concepts/covalues",
    "",
    "CoValues are Jazz's collaborative values. A CoValue can be a CoMap, CoList, or CoFeed.",
  );
  insertSection.run(
    "CoValues",
    "core-concepts/covalues",
    "CoMap",
    "Use CoMap to define collaborative key-value objects. CoMaps are the most common CoValue type.",
  );
  insertSection.run(
    "CoValues",
    "core-concepts/covalues",
    "CoList",
    "Use CoList for ordered collections. CoLists work like JavaScript arrays for storing CoValues.",
  );

  // Page 3: Subscriptions & Deep Loading
  insertPage.run(
    "Subscriptions & Deep Loading",
    "core-concepts/subscription-and-loading",
    "Learn how to subscribe to CoValues and handle loading states.",
    "Jazz's CoValues are reactive. Subscribe to a CoValue to receive updates whenever it changes.",
  );
  insertSection.run(
    "Subscriptions & Deep Loading",
    "core-concepts/subscription-and-loading",
    "",
    "Jazz's CoValues are reactive. Subscribe to a CoValue to receive updates whenever it changes.",
  );
  insertSection.run(
    "Subscriptions & Deep Loading",
    "core-concepts/subscription-and-loading",
    "Subscription Hooks",
    "Use useCoState to subscribe to a CoValue in React. Jazz automatically handles subscriptions and cleanup.",
  );
  insertSection.run(
    "Subscriptions & Deep Loading",
    "core-concepts/subscription-and-loading",
    "Deep Loading",
    "Load CoValues deeply by resolving nested CoValues. Specify exactly how much data to subscribe to.",
  );

  db.close();
});

afterEach(async () => {
  await rm(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

describe("createSqliteBackend", () => {
  it("returns an object with search, getDoc, listPages methods", async () => {
    const backend = await createSqliteBackend(dbPath);
    expect(typeof backend.search).toBe("function");
    expect(typeof backend.getDoc).toBe("function");
    expect(typeof backend.listPages).toBe("function");
  });
});

// ---------------------------------------------------------------------------
// search()
// ---------------------------------------------------------------------------

describe("search()", () => {
  it("returns results matching the query", async () => {
    const backend = await createSqliteBackend(dbPath);
    const results = backend.search("install", 10);
    expect(results.length).toBeGreaterThan(0);
    expect(results[0]!.slug).toBe("quickstart");
  });

  it("result shape has title, slug, section, snippet", async () => {
    const backend = await createSqliteBackend(dbPath);
    const result = backend.search("install", 1)[0]!;
    expect(typeof result.title).toBe("string");
    expect(typeof result.slug).toBe("string");
    expect(typeof result.section).toBe("string");
    expect(typeof result.snippet).toBe("string");
  });

  it("section maps to the section_heading of the matching row", async () => {
    const backend = await createSqliteBackend(dbPath);
    const result = backend.search("install", 1)[0]!;
    expect(result.section).toBe("Install Jazz");
  });

  it("respects the limit parameter", async () => {
    const backend = await createSqliteBackend(dbPath);
    // "CoValue" appears in multiple sections across pages
    const results = backend.search("CoValue", 2);
    expect(results.length).toBeLessThanOrEqual(2);
  });

  it("returns empty array when no matches", async () => {
    const backend = await createSqliteBackend(dbPath);
    const results = backend.search("xyzzy_no_match_at_all", 10);
    expect(results).toEqual([]);
  });

  it("orders results by relevance (most relevant first)", async () => {
    const backend = await createSqliteBackend(dbPath);
    // "useCoState" appears exclusively in subscription-and-loading
    const results = backend.search("useCoState", 10);
    expect(results[0]!.slug).toBe("core-concepts/subscription-and-loading");
  });
});

// ---------------------------------------------------------------------------
// getDoc()
// ---------------------------------------------------------------------------

describe("getDoc()", () => {
  it("returns body and related for a known slug", async () => {
    const backend = await createSqliteBackend(dbPath);
    const doc = backend.getDoc("quickstart");
    expect(doc).not.toBeNull();
    expect(typeof doc!.body).toBe("string");
    expect(doc!.body.length).toBeGreaterThan(0);
    expect(Array.isArray(doc!.related)).toBe(true);
  });

  it("returns null for an unknown slug", async () => {
    const backend = await createSqliteBackend(dbPath);
    expect(backend.getDoc("does-not-exist")).toBeNull();
  });

  it("body is the full page body from the pages table", async () => {
    const backend = await createSqliteBackend(dbPath);
    const doc = backend.getDoc("core-concepts/covalues");
    expect(doc!.body).toContain("CoValue");
  });

  it("related does not include self", async () => {
    const backend = await createSqliteBackend(dbPath);
    const doc = backend.getDoc("core-concepts/covalues");
    expect(doc!.related).not.toContain("core-concepts/covalues");
  });

  it("related has at most 5 entries", async () => {
    const backend = await createSqliteBackend(dbPath);
    const doc = backend.getDoc("core-concepts/covalues");
    expect(doc!.related.length).toBeLessThanOrEqual(5);
  });

  it("related finds pages that share significant terms", async () => {
    const backend = await createSqliteBackend(dbPath);
    // covalues and subscription-and-loading both heavily mention "CoValue"
    const doc = backend.getDoc("core-concepts/covalues");
    expect(doc!.related).toContain("core-concepts/subscription-and-loading");
  });
});

// ---------------------------------------------------------------------------
// listPages()
// ---------------------------------------------------------------------------

describe("listPages()", () => {
  it("returns all pages", async () => {
    const backend = await createSqliteBackend(dbPath);
    const pages = backend.listPages();
    expect(pages.length).toBe(3);
  });

  it("each entry has title, slug, description", async () => {
    const backend = await createSqliteBackend(dbPath);
    const page = backend.listPages()[0]!;
    expect(typeof page.title).toBe("string");
    expect(typeof page.slug).toBe("string");
    expect(typeof page.description).toBe("string");
  });

  it("description comes from the pages table (not derived)", async () => {
    const backend = await createSqliteBackend(dbPath);
    const pages = backend.listPages();
    const qs = pages.find((p) => p.slug === "quickstart")!;
    expect(qs.description).toBe(
      "Get started building a simple front-end app with Jazz in 10 minutes.",
    );
  });
});

// ---------------------------------------------------------------------------
// Top-level import safety
// ---------------------------------------------------------------------------

describe("module import safety", () => {
  it("the module source does not contain a top-level node:sqlite import", async () => {
    // Read the source file and verify there is no top-level static import of node:sqlite.
    // This is a static analysis check — the runtime test is that the module loads fine
    // even when node:sqlite is unavailable (which we can't easily simulate here).
    const { readFile } = await import("node:fs/promises");
    const { fileURLToPath } = await import("node:url");
    const { dirname, join: pathJoin } = await import("node:path");
    const here = dirname(fileURLToPath(import.meta.url));
    const src = await readFile(pathJoin(here, "backend-sqlite.ts"), "utf8");
    // Top-level static imports look like: import ... from "node:sqlite"
    const topLevelImportRe = /^import\s+.*from\s+["']node:sqlite["']/m;
    expect(topLevelImportRe.test(src)).toBe(false);
  });
});
