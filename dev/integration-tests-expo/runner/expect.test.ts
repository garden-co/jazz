import { describe, it, expect as v } from "vitest";
import { expect } from "./expect";

describe("expect shim", () => {
  it("toBe / not.toBe", () => {
    expect(2).toBe(2);
    expect(2).not.toBe(3);
    v(() => expect(2).toBe(3)).toThrow();
    v(() => expect(2).not.toBe(2)).toThrow();
  });

  it("toEqual deep", () => {
    expect({ a: [1, { b: 2 }] }).toEqual({ a: [1, { b: 2 }] });
    expect([1, 2, 3]).toEqual([1, 2, 3]);
    v(() => expect({ a: 1 }).toEqual({ a: 2 })).toThrow();
    v(() => expect({ a: 1 }).toEqual({ a: 1, b: 2 })).toThrow();
  });

  it("toMatchObject subset", () => {
    expect({ id: "x", title: "t", done: false }).toMatchObject({ title: "t", done: false });
    expect({ nested: { a: 1, b: 2 } }).toMatchObject({ nested: { a: 1 } });
    v(() => expect({ title: "t" }).toMatchObject({ title: "z" })).toThrow();
  });

  it("toHaveLength / toContain", () => {
    expect([1, 2, 3]).toHaveLength(3);
    expect([1, 2, 3]).toContain(2);
    expect("hello").toContain("ell");
    expect([{ id: "a" }]).toContain({ id: "a" });
    v(() => expect([1]).toHaveLength(2)).toThrow();
    v(() => expect([1, 2]).toContain(9)).toThrow();
  });

  it("comparisons and nullish", () => {
    expect(5).toBeGreaterThan(4);
    expect(5).toBeGreaterThanOrEqual(5);
    expect(null).toBeNull();
    expect(1).toBeDefined();
    expect(undefined).toBeUndefined();
    expect(0).toBeFalsy();
    expect("x").toBeTruthy();
    v(() => expect(3).toBeGreaterThan(4)).toThrow();
  });
});
