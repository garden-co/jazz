import { beforeAll, describe, test, expect } from "vitest";
import {
  assertLoaded,
  co,
  createUnloadedCoValue,
  CoValueLoadingState,
  getLoadedOrUndefined,
} from "../internal";
import { createJazzTestAccount, setupJazzTestSync } from "../testing";

beforeAll(async () => {
  await setupJazzTestSync();
  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("assertLoaded", () => {
  test("should throw an error if the CoValue is not loaded", () => {
    const coValue = createUnloadedCoValue(
      "id",
      CoValueLoadingState.UNAVAILABLE,
    );
    expect(() => assertLoaded(coValue)).toThrow("CoValue is not loaded");
  });

  test("should narrow the value if it is loaded", () => {
    const coValue = co.plainText().create("value");
    assertLoaded(coValue);
    expect(coValue.$isLoaded).toBe(true);
    expect(coValue.toUpperCase()).toBe("VALUE");
  });
});

describe("getLoadedOrUndefined", () => {
  test("should return undefined if the CoValue is not loaded", () => {
    const coValue = createUnloadedCoValue(
      "id",
      CoValueLoadingState.UNAVAILABLE,
    );
    expect(getLoadedOrUndefined(coValue)).toBeUndefined();
  });

  test("should return the CoValue if it is loaded", () => {
    const coValue = co.plainText().create("value");
    expect(getLoadedOrUndefined(coValue)).toBe(coValue);
  });

  test("should return undefined if the CoValue is null", () => {
    expect(getLoadedOrUndefined(null)).toBeUndefined();
  });

  test("should return undefined if the CoValue is undefined", () => {
    expect(getLoadedOrUndefined(undefined)).toBeUndefined();
  });
});
