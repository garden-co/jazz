import { describe, it, expect } from "vitest";
import { detectPackageManager } from "./detect-pm.js";

describe("detectPackageManager", () => {
  it("returns the package manager name when the user agent starts with one", () => {
    expect(detectPackageManager("pnpm/9.15.0 npm/? node/v22.0.0 darwin x64")).toBe("pnpm");
  });

  it("returns null when the user agent is missing or unrecognised", () => {
    expect(detectPackageManager(undefined)).toBeNull();
    expect(detectPackageManager("deno/1.0.0 node/v20.0.0")).toBeNull();
  });
});
