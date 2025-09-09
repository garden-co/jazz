import { describe, expect, test } from "vitest";
import { pick } from "./utils";

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
