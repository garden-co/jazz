import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createNaiveBackend } from "./backend-naive.js";

// ---------------------------------------------------------------------------
// Fixture txt — exact format produced by buildIndex
//
// Three pages reflecting jazz1 docs structure:
//   quickstart                            — installing + schema definition
//   core-concepts/covalues                — CoMap + CoList  (mentions "CoValue" often)
//   core-concepts/subscription-and-loading — subscribing + deep loading (mentions "CoValue" too)
// ---------------------------------------------------------------------------

const FIXTURE_TXT = `===PAGE:quickstart===
TITLE:Quickstart
DESCRIPTION:Get started building a simple front-end app with Jazz in 10 minutes.

Build your first Jazz app. The jazz-tools package includes everything you need to install and run Jazz.

## Install Jazz

Install jazz-tools to get started. Run npm install jazz-tools to install the package.

## Set Up Your Schema

Define a CoMap schema to describe your app's data structure using co.map().

===PAGE:core-concepts/covalues===
TITLE:CoValues
DESCRIPTION:CoValues are the collaborative data types at the core of Jazz.

CoValues are Jazz's collaborative values. A CoValue can be a CoMap, CoList, or CoFeed.

## CoMap

Use CoMap to define collaborative key-value objects. CoMaps are the most common CoValue type.

## CoList

Use CoList for ordered collections. CoLists work like JavaScript arrays for storing CoValues.

===PAGE:core-concepts/subscription-and-loading===
TITLE:Subscriptions & Deep Loading
DESCRIPTION:Learn how to subscribe to CoValues and handle loading states.

Jazz's CoValues are reactive. Subscribe to a CoValue to receive updates whenever it changes.

## Subscription Hooks

Use useCoState to subscribe to a CoValue in React. Jazz automatically handles subscriptions and cleanup.

## Deep Loading

Load CoValues deeply by resolving nested CoValues. Specify exactly how much data to subscribe to.`;

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
    const spy = vi
      .spyOn(process.stderr, "write")
      .mockImplementation(() => true);
    await createNaiveBackend(txtPath);
    const calls = spy.mock.calls.map((c) => String(c[0]));
    expect(calls.some((msg) => msg.includes("node:sqlite not available"))).toBe(
      true,
    );
  });

  it("warning message mentions upgrade path", async () => {
    const spy = vi
      .spyOn(process.stderr, "write")
      .mockImplementation(() => true);
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
    const results = backend.search("install", 10);
    expect(results.length).toBeGreaterThan(0);
    expect(results.every((r) => r.slug === "quickstart")).toBe(true);
  });

  it("result shape has title, slug, section, snippet", async () => {
    const backend = await createNaiveBackend(txtPath);
    const result = backend.search("install", 1)[0]!;
    expect(typeof result.title).toBe("string");
    expect(typeof result.slug).toBe("string");
    expect(typeof result.section).toBe("string");
    expect(typeof result.snippet).toBe("string");
  });

  it("section field maps to the ## heading of the matching section", async () => {
    const backend = await createNaiveBackend(txtPath);
    const result = backend.search("install", 1)[0]!;
    expect(result.section).toBe("Install Jazz");
  });

  it("ALL query terms must appear in a section for it to match", async () => {
    const backend = await createNaiveBackend(txtPath);
    // "install" is in quickstart, "subscribe" is in subscription-and-loading
    // No single section contains both
    const results = backend.search("install subscribe", 10);
    expect(results).toHaveLength(0);
  });

  it("returns empty array when no section matches", async () => {
    const backend = await createNaiveBackend(txtPath);
    expect(backend.search("xyzzy_no_match_at_all", 10)).toEqual([]);
  });

  it("respects the limit parameter", async () => {
    const backend = await createNaiveBackend(txtPath);
    // "CoValue" appears in multiple sections across pages
    const results = backend.search("CoValue", 2);
    expect(results.length).toBeLessThanOrEqual(2);
  });

  it("ranks by term frequency — more occurrences ranks higher", async () => {
    const backend = await createNaiveBackend(txtPath);
    // "install" appears twice in the Install Jazz section body
    const results = backend.search("install", 10);
    // The Install Jazz section (with "install" twice) should rank first
    expect(results[0]!.section).toBe("Install Jazz");
  });

  it("snippet is a non-empty string", async () => {
    const backend = await createNaiveBackend(txtPath);
    const result = backend.search("install", 1)[0]!;
    expect(result.snippet.length).toBeGreaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// getDoc()
// ---------------------------------------------------------------------------

describe("getDoc()", () => {
  it("returns body and empty related for a known slug", async () => {
    const backend = await createNaiveBackend(txtPath);
    const doc = backend.getDoc("quickstart");
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
    const doc = backend.getDoc("core-concepts/covalues");
    expect(doc!.body).toContain("CoMap");
    expect(doc!.body).toContain("CoList");
  });

  it("related is always an empty array", async () => {
    const backend = await createNaiveBackend(txtPath);
    // Even for pages that share terms, related must be []
    expect(backend.getDoc("core-concepts/covalues")!.related).toEqual([]);
    expect(
      backend.getDoc("core-concepts/subscription-and-loading")!.related,
    ).toEqual([]);
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
    const page = backend.listPages()[0]!;
    expect(typeof page.title).toBe("string");
    expect(typeof page.slug).toBe("string");
    expect(typeof page.description).toBe("string");
  });

  it("title and description are parsed from the txt markers", async () => {
    const backend = await createNaiveBackend(txtPath);
    const pages = backend.listPages();
    const qs = pages.find((p) => p.slug === "quickstart")!;
    expect(qs.title).toBe("Quickstart");
    expect(qs.description).toBe(
      "Get started building a simple front-end app with Jazz in 10 minutes.",
    );
  });

  it("all three fixture slugs are present", async () => {
    const backend = await createNaiveBackend(txtPath);
    const slugs = backend.listPages().map((p) => p.slug);
    expect(slugs).toContain("quickstart");
    expect(slugs).toContain("core-concepts/covalues");
    expect(slugs).toContain("core-concepts/subscription-and-loading");
  });
});
