// @vitest-environment happy-dom

import { renderHook as renderHookRaw } from "@testing-library/react";
import { beforeEach, describe, expect, it } from "vitest";
import { useJazzProviderCheck } from "../hooks.js";
import { renderHook } from "./testUtils.js";
import { createJazzTestAccount } from "../testing.js";

beforeEach(() => {
  createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("useJazzProviderCheck", () => {
  it("should not throw when used outside a JazzProvider", () => {
    // Using raw renderHook without JazzTestProvider wrapper
    expect(() => {
      renderHookRaw(() => useJazzProviderCheck());
    }).not.toThrow();
  });

  it("should throw when used inside a JazzProvider", () => {
    // Using custom renderHook that wraps with JazzTestProvider
    expect(() => {
      renderHook(() => useJazzProviderCheck());
    }).toThrow("You can't nest a JazzProvider inside another JazzProvider.");
  });
});
