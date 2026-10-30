import { describe, expect, it } from "vitest";
import { comparePositioned, positionBetween } from "../../src/lib/positions";
import { applyTextSplice, textSplice } from "../../src/lib/text-splice";
import { buildPageTree } from "../../src/lib/tree";
import { seedId } from "../../src/lib/bootstrap";

describe("textSplice", () => {
  const cases: [string, string][] = [
    ["", "hello"],
    ["hello", ""],
    ["salt on the window", "salt on the cold window"],
    ["abcabc", "abc"],
    ["ferry's late", "ferry is late"],
    ["tide 🌊 turns", "tide 🌙 turns"],
    ["🎸", "🎸🎸"],
  ];
  it.each(cases)("turns %j into %j with one minimal splice", (base, next) => {
    const splice = textSplice(base, next)!;
    expect(applyTextSplice(base, splice)).toBe(next);
    // Never cut a surrogate pair, which Jazz would reject.
    const edited = base.slice(splice.at, splice.at + splice.delete) + splice.insert;
    expect(edited).toBe(edited.normalize());
    expect(/[\uD800-\uDBFF]$/.test(base.slice(0, splice.at))).toBe(false);
  });
  it("returns null when nothing changed", () => {
    expect(textSplice("same", "same")).toBeNull();
  });
});

describe("positions", () => {
  it("always finds room between two neighbours", () => {
    expect(positionBetween(undefined, undefined)).toBeGreaterThan(0);
    expect(positionBetween(1024, undefined)).toBeGreaterThan(1024);
    expect(positionBetween(undefined, 1024)).toBeLessThan(1024);
    const middle = positionBetween(1024, 2048);
    expect(middle).toBeGreaterThan(1024);
    expect(middle).toBeLessThan(2048);
  });
  it("breaks position ties by creation time, then id", () => {
    const rows = [
      { id: "b", position: 1, $createdAt: 20 },
      { id: "a", position: 1, $createdAt: 20 },
      { id: "c", position: 1, $createdAt: 10 },
      { id: "d", position: 0, $createdAt: 30 },
    ];
    expect([...rows].sort(comparePositioned).map((row) => row.id)).toEqual(["d", "c", "a", "b"]);
  });
});

describe("page tree", () => {
  const pages = [
    { id: "songs", parentId: null, title: "Songs", kind: "doc" as const },
    { id: "harbour", parentId: "songs", title: "Harbour lights", kind: "doc" as const },
    { id: "notes", parentId: "harbour", title: "Arrangement", kind: "doc" as const },
    { id: "tour", parentId: null, title: "Tour", kind: "doc" as const },
  ];
  const tree = buildPageTree(pages);

  it("keeps the query's order for siblings and nests children", () => {
    expect(tree.roots.map((page) => page.id)).toEqual(["songs", "tour"]);
    expect(tree.ancestors("notes").map((page) => page.id)).toEqual(["songs", "harbour"]);
    expect([...tree.descendantIds("songs")].sort()).toEqual(["harbour", "notes"]);
    expect(tree.depth("notes")).toBe(2);
  });

  it("treats a page whose parent is hidden as a root", () => {
    const guestView = buildPageTree(pages.filter((page) => page.id !== "songs"));
    expect(guestView.roots.map((page) => page.id)).toEqual(["harbour", "tour"]);
  });
});

describe("seed ids", () => {
  it("are stable per account and valid version 5 UUIDs", () => {
    const account = "7b0c6b1e-3a1d-4a8e-9a55-2f1d5b6c7d8e";
    expect(seedId(account, "workspace")).toBe(seedId(account, "workspace"));
    expect(seedId(account, "workspace")).not.toBe(seedId(account, "page:setlist"));
    expect(seedId(account, "workspace")).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
  });
});
