import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createNaiveBackend } from "./backend-naive.js";

function firstResult<T>(values: T[]): T {
  expect(values.length).toBeGreaterThan(0);
  return values[0]!;
}

// ---------------------------------------------------------------------------
// Fixture txt — exact format produced by buildIndex
//
// Three pages matching the SQLite backend tests for easy comparison:
//   getting-started  — installation + configuration
//   reading-data     — subscriptions + queries  (mentions "data" often)
//   writing-data     — mutations + transactions (mentions "data" often)
// ---------------------------------------------------------------------------

const FIXTURE_TXT = `===PAGE:getting-started===
TITLE:Getting Started
DESCRIPTION:Learn how to install and configure Jazz.

This guide walks you through installation and initial configuration of Jazz.

## Installation

Run npm install jazz-tools to install the package. Installation is straightforward.

## Configuration

Configure your app by passing options to createJazzClient.

===PAGE:reading-data===
TITLE:Reading Data
DESCRIPTION:Query APIs for reading data from Jazz.

Jazz provides query APIs for reading data. Use subscriptions for reactive updates.

## Subscriptions

Subscribe to data changes using useAll or QuerySubscription. Data flows reactively.

## Queries

Build type-safe queries with the query builder for reading data. Query data efficiently.

===PAGE:writing-data===
TITLE:Writing Data
DESCRIPTION:APIs for writing and mutating data in Jazz.

Writing data in Jazz uses mutations. Transactions ensure consistency when writing data.

## Mutations

Use db.create and db.update to write data to Jazz. Mutations are atomic.

## Transactions

Wrap multiple writes in a transaction for consistency when writing data.`;

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

let tmpDir: string;
let txtPath: string;

beforeEach(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "backend-naive-test-"));
  txtPath = join(tmpDir, "docs-index.txt");
  await writeFile(txtPath, FIXTURE_TXT, "utf8");
});

afterEach(async () => {
  await rm(tmpDir, { recursive: true, force: true });
  vi.restoreAllMocks();
});

// ---------------------------------------------------------------------------
// Factory + stderr warning
// ---------------------------------------------------------------------------

describe("createNaiveBackend", () => {
  it("returns an object with search, getDoc, listPages methods", async () => {
    const backend = await createNaiveBackend(txtPath);
    expect(typeof backend.search).toBe("function");
    expect(typeof backend.getDoc).toBe("function");
    expect(typeof backend.listPages).toBe("function");
  });

  it("emits the node:sqlite unavailable warning to stderr on construction", async () => {
    const spy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await createNaiveBackend(txtPath);
    const calls = spy.mock.calls.map((c) => String(c[0]));
    expect(calls.some((msg) => msg.includes("node:sqlite not available"))).toBe(true);
  });

  it("warning message mentions upgrade path", async () => {
    const spy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await createNaiveBackend(txtPath);
    const calls = spy.mock.calls.map((c) => String(c[0]));
    expect(calls.some((msg) => msg.includes("Node >=22.13"))).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// search()
// ---------------------------------------------------------------------------

describe("search()", () => {
  it("returns sections containing all query terms (case-insensitive)", async () => {
    const backend = await createNaiveBackend(txtPath);
    const results = backend.search("installation", 10);
    expect(results.length).toBeGreaterThan(0);
    expect(results.every((r) => r.slug === "getting-started")).toBe(true);
  });

  it("result shape has title, slug, section, snippet", async () => {
    const backend = await createNaiveBackend(txtPath);
    const result = firstResult(backend.search("installation", 1));
    expect(typeof result.title).toBe("string");
    expect(typeof result.slug).toBe("string");
    expect(typeof result.section).toBe("string");
    expect(typeof result.snippet).toBe("string");
  });

  it("section maps to the ## heading of the matching section", async () => {
    const backend = await createNaiveBackend(txtPath);
    const result = firstResult(backend.search("installation", 1));
    expect(result.section).toBe("Installation");
  });

  it("ALL query terms must appear in a section for it to match", async () => {
    const backend = await createNaiveBackend(txtPath);
    // "installation" is in getting-started, "subscriptions" is in reading-data
    // No single section contains both
    const results = backend.search("installation subscriptions", 10);
    expect(results).toHaveLength(0);
  });

  it("returns empty array when no section matches", async () => {
    const backend = await createNaiveBackend(txtPath);
    expect(backend.search("xyzzy_no_match_at_all", 10)).toEqual([]);
  });

  it("respects the limit parameter", async () => {
    const backend = await createNaiveBackend(txtPath);
    // "data" appears in multiple sections across pages
    const results = backend.search("data", 2);
    expect(results.length).toBeLessThanOrEqual(2);
  });

  it("ranks by term frequency — more occurrences ranks higher", async () => {
    const backend = await createNaiveBackend(txtPath);
    // "installation" appears twice in the Installation section body
    // but only implicitly in the preamble
    const results = backend.search("installation", 10);
    // The Installation section (with "installation" twice) should rank first
    expect(results[0]!.section).toBe("Installation");
  });

  it("snippet is a non-empty string", async () => {
    const backend = await createNaiveBackend(txtPath);
    const result = firstResult(backend.search("installation", 1));
    expect(result.snippet.length).toBeGreaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// getDoc()
// ---------------------------------------------------------------------------

describe("getDoc()", () => {
  it("returns body and empty related for a known slug", async () => {
    const backend = await createNaiveBackend(txtPath);
    const doc = backend.getDoc("getting-started");
    expect(doc).not.toBeNull();
    expect(typeof doc!.body).toBe("string");
    expect(doc!.body.length).toBeGreaterThan(0);
    expect(doc!.related).toEqual([]);
  });

  it("returns null for an unknown slug", async () => {
    const backend = await createNaiveBackend(txtPath);
    expect(backend.getDoc("does-not-exist")).toBeNull();
  });

  it("body contains the page's full content including section headings", async () => {
    const backend = await createNaiveBackend(txtPath);
    const doc = backend.getDoc("reading-data");
    expect(doc!.body).toContain("Subscriptions");
    expect(doc!.body).toContain("Queries");
  });

  it("related is always an empty array", async () => {
    const backend = await createNaiveBackend(txtPath);
    // Even for pages that share terms, related must be []
    expect(backend.getDoc("reading-data")!.related).toEqual([]);
    expect(backend.getDoc("writing-data")!.related).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// listPages()
// ---------------------------------------------------------------------------

describe("listPages()", () => {
  it("returns all pages", async () => {
    const backend = await createNaiveBackend(txtPath);
    expect(backend.listPages()).toHaveLength(3);
  });

  it("each entry has title, slug, description", async () => {
    const backend = await createNaiveBackend(txtPath);
    const page = firstResult(backend.listPages());
    expect(typeof page.title).toBe("string");
    expect(typeof page.slug).toBe("string");
    expect(typeof page.description).toBe("string");
  });

  it("title and description are parsed from the txt markers", async () => {
    const backend = await createNaiveBackend(txtPath);
    const pages = backend.listPages();
    const gs = pages.find((p) => p.slug === "getting-started")!;
    expect(gs.title).toBe("Getting Started");
    expect(gs.description).toBe("Learn how to install and configure Jazz.");
  });

  it("all three fixture slugs are present", async () => {
    const backend = await createNaiveBackend(txtPath);
    const slugs = backend.listPages().map((p) => p.slug);
    expect(slugs).toContain("getting-started");
    expect(slugs).toContain("reading-data");
    expect(slugs).toContain("writing-data");
  });
});
