import { describe, expect, test } from "vitest";
import { pick, structuralEquals } from "./utils";

describe("pick", () => {
  test("should pick the requested keys from an object", () => {
    const obj = { a: 1, b: 2, c: 3 };
    const picked = pick(obj, ["a", "c"]);
    expect(picked).toEqual({ a: 1, c: 3 });
  });

  test("should not allow keys that are not in the object", () => {
    const obj = { a: 1, b: 2, c: 3 };
    // @ts-expect-error - d is not in the object
    const picked = pick(obj, ["d"]);
    expect(picked).toEqual({});
  });
});

describe("structuralEquals", () => {
  test("should return true for two equal objects", () => {
    const obj1 = { a: 1, b: 2, c: 3 };
    const obj2 = { a: 1, b: 2, c: 3 };
    expect(structuralEquals(obj1, obj2)).toBe(true);
  });

  test("should return false for two different objects", () => {
    const obj1 = { a: 1, b: 2, c: 3 };
    const obj2 = { a: 1, b: 2, c: 4 };
    expect(structuralEquals(obj1, obj2)).toBe(false);
  });

  test("should return true for two equal arrays", () => {
    const arr1 = [1, 2, 3];
    const arr2 = [1, 2, 3];
    expect(structuralEquals(arr1, arr2)).toBe(true);
  });

  test("should return false for two different arrays", () => {
    const arr1 = [1, 2, 3];
    const arr2 = [1, 2, 4];
    expect(structuralEquals(arr1, arr2)).toBe(false);
  });

  test("should return true for two equal numbers", () => {
    const num1 = 1;
    const num2 = 1;
    expect(structuralEquals(num1, num2)).toBe(true);
  });

  test("should return false for two different numbers", () => {
    const num1 = 1;
    const num2 = 2;
    expect(structuralEquals(num1, num2)).toBe(false);
  });

  test("should return true for two equal strings", () => {
    const str1 = "hello";
    const str2 = "hello";
    expect(structuralEquals(str1, str2)).toBe(true);
  });

  test("should return false for two different strings", () => {
    const str1 = "hello";
    const str2 = "world";
    expect(structuralEquals(str1, str2)).toBe(false);
  });

  test("should return true for two equal booleans", () => {
    const bool1 = true;
    const bool2 = true;
    expect(structuralEquals(bool1, bool2)).toBe(true);
  });

  test("should return false for two different booleans", () => {
    const bool1 = true;
    const bool2 = false;
    expect(structuralEquals(bool1, bool2)).toBe(false);
  });

  test("should return true for two equal nulls", () => {
    const null1 = null;
    const null2 = null;
    expect(structuralEquals(null1, null2)).toBe(true);
  });

  test("should return false for two different nulls", () => {
    const null1 = null;
    const null2 = undefined;
    expect(structuralEquals(null1, null2)).toBe(false);
  });

  test("should return true for two equal undefined", () => {
    const undefined1 = undefined;
    const undefined2 = undefined;
    expect(structuralEquals(undefined1, undefined2)).toBe(true);
  });

  test("should return false for two different undefined", () => {
    const undefined1 = undefined;
    const undefined2 = null;
    expect(structuralEquals(undefined1, undefined2)).toBe(false);
  });

  test("should return true for two equal NaN", () => {
    const nan1 = NaN;
    const nan2 = NaN;
    expect(structuralEquals(nan1, nan2)).toBe(true);
  });

  test("should return false for two different NaN", () => {
    const nan1 = NaN;
    const nan2 = 0;
    expect(structuralEquals(nan1, nan2)).toBe(false);
  });

  test("should return true for two equal Infinity", () => {
    const infinity1 = Infinity;
    const infinity2 = Infinity;
    expect(structuralEquals(infinity1, infinity2)).toBe(true);
  });

  test("should return false for two different Infinity", () => {
    const infinity1 = Infinity;
    const infinity2 = -Infinity;
    expect(structuralEquals(infinity1, infinity2)).toBe(false);
  });

  test("should return true for two equal nested objects", () => {
    const obj1 = { a: 1, b: 2, c: { d: [3, 4, 5] } };
    const obj2 = { a: 1, b: 2, c: { d: [3, 4, 5] } };
    expect(structuralEquals(obj1, obj2)).toBe(true);
  });

  test("should return false for two different nested objects", () => {
    const obj1 = { a: 1, b: 2, c: { d: [3, 4, 5] } };
    const obj2 = { a: 1, b: 2, c: { d: [3, 4, 6] } };
    expect(structuralEquals(obj1, obj2)).toBe(false);
  });
});
