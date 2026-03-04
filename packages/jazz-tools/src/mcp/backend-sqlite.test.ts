import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { createSqliteBackend } from "./backend-sqlite.js";

// ---------------------------------------------------------------------------
// Fixture DB
//
// Three pages:
//   "getting-started"  — installation and configuration
//   "reading-data"     — subscriptions and queries  (shares "data" theme)
//   "writing-data"     — mutations and transactions (shares "data" theme)
//
// ASCII map of expected related links:
//   reading-data ←→ writing-data  (both about "data" operations)
//   getting-started is unrelated to the others
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
  const insertSection = db.prepare("INSERT INTO sections_fts VALUES (?, ?, ?, ?)");

  // Page 1: Getting Started
  insertPage.run(
    "Getting Started",
    "getting-started",
    "Learn how to install and configure Jazz.",
    "This guide walks you through installation and initial configuration of Jazz.",
  );
  insertSection.run(
    "Getting Started",
    "getting-started",
    "",
    "This guide walks you through installation and initial configuration of Jazz.",
  );
  insertSection.run(
    "Getting Started",
    "getting-started",
    "Installation",
    "Run npm install jazz-tools to install the package.",
  );
  insertSection.run(
    "Getting Started",
    "getting-started",
    "Configuration",
    "Configure your app by passing options to createJazzClient.",
  );

  // Page 2: Reading Data
  insertPage.run(
    "Reading Data",
    "reading-data",
    "Query APIs for reading data from Jazz.",
    "Jazz provides query APIs for reading data. Use subscriptions for reactive updates. Queries return typed results.",
  );
  insertSection.run(
    "Reading Data",
    "reading-data",
    "",
    "Jazz provides query APIs for reading data.",
  );
  insertSection.run(
    "Reading Data",
    "reading-data",
    "Subscriptions",
    "Subscribe to data changes using useAll or QuerySubscription.",
  );
  insertSection.run(
    "Reading Data",
    "reading-data",
    "Queries",
    "Build type-safe queries with the query builder for reading data.",
  );

  // Page 3: Writing Data
  insertPage.run(
    "Writing Data",
    "writing-data",
    "APIs for writing and mutating data in Jazz.",
    "Writing data in Jazz uses mutations. Transactions ensure consistency when writing data.",
  );
  insertSection.run("Writing Data", "writing-data", "", "Writing data in Jazz uses mutations.");
  insertSection.run(
    "Writing Data",
    "writing-data",
    "Mutations",
    "Use db.create and db.update to write data to Jazz.",
  );
  insertSection.run(
    "Writing Data",
    "writing-data",
    "Transactions",
    "Wrap multiple writes in a transaction for consistency.",
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
    const results = backend.search("installation", 10);
    expect(results.length).toBeGreaterThan(0);
    expect(results[0].slug).toBe("getting-started");
  });

  it("result shape has title, slug, section, snippet", async () => {
    const backend = await createSqliteBackend(dbPath);
    const [result] = backend.search("installation", 1);
    expect(typeof result.title).toBe("string");
    expect(typeof result.slug).toBe("string");
    expect(typeof result.section).toBe("string");
    expect(typeof result.snippet).toBe("string");
  });

  it("section maps to the section_heading of the matching row", async () => {
    const backend = await createSqliteBackend(dbPath);
    const [result] = backend.search("installation", 1);
    expect(result.section).toBe("Installation");
  });

  it("respects the limit parameter", async () => {
    const backend = await createSqliteBackend(dbPath);
    const results = backend.search("data", 2);
    expect(results.length).toBeLessThanOrEqual(2);
  });

  it("returns empty array when no matches", async () => {
    const backend = await createSqliteBackend(dbPath);
    const results = backend.search("xyzzy_no_match_at_all", 10);
    expect(results).toEqual([]);
  });

  it("orders results by relevance (most relevant first)", async () => {
    const backend = await createSqliteBackend(dbPath);
    // "subscriptions" appears exclusively in reading-data
    const results = backend.search("subscriptions", 10);
    expect(results[0].slug).toBe("reading-data");
  });
});

// ---------------------------------------------------------------------------
// getDoc()
// ---------------------------------------------------------------------------

describe("getDoc()", () => {
  it("returns body and related for a known slug", async () => {
    const backend = await createSqliteBackend(dbPath);
    const doc = backend.getDoc("getting-started");
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
    const doc = backend.getDoc("reading-data");
    expect(doc!.body).toContain("subscriptions");
  });

  it("related does not include self", async () => {
    const backend = await createSqliteBackend(dbPath);
    const doc = backend.getDoc("reading-data");
    expect(doc!.related).not.toContain("reading-data");
  });

  it("related has at most 5 entries", async () => {
    const backend = await createSqliteBackend(dbPath);
    const doc = backend.getDoc("reading-data");
    expect(doc!.related.length).toBeLessThanOrEqual(5);
  });

  it("related finds pages that share significant terms", async () => {
    const backend = await createSqliteBackend(dbPath);
    // reading-data and writing-data both heavily mention "data"
    const doc = backend.getDoc("reading-data");
    expect(doc!.related).toContain("writing-data");
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
    const [page] = backend.listPages();
    expect(typeof page.title).toBe("string");
    expect(typeof page.slug).toBe("string");
    expect(typeof page.description).toBe("string");
  });

  it("description comes from the pages table (not derived)", async () => {
    const backend = await createSqliteBackend(dbPath);
    const pages = backend.listPages();
    const gs = pages.find((p) => p.slug === "getting-started")!;
    expect(gs.description).toBe("Learn how to install and configure Jazz.");
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
