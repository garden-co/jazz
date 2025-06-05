import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { getPkgManager } from "../src/utils.js";

describe("Package Manager Detection", () => {
  let originalUserAgent: string | undefined;

  beforeEach(() => {
    // Save the original value
    originalUserAgent = process.env.npm_config_user_agent;
  });

  afterEach(() => {
    // Restore the original value
    if (originalUserAgent !== undefined) {
      process.env.npm_config_user_agent = originalUserAgent;
    } else {
      delete process.env.npm_config_user_agent;
    }
  });

  it("should detect pnpm when run via pnpm dlx", () => {
    // Simulate the environment variable that pnpm sets
    process.env.npm_config_user_agent =
      "pnpm/8.15.4 npm/? node/v20.11.1 darwin x64";

    const result = getPkgManager();
    expect(result).toBe("pnpm");
  });

  it("should detect yarn when run via yarn", () => {
    process.env.npm_config_user_agent =
      "yarn/1.22.19 npm/? node/v20.11.1 darwin x64";

    const result = getPkgManager();
    expect(result).toBe("yarn");
  });

  it("should detect bun when run via bun", () => {
    process.env.npm_config_user_agent =
      "bun/1.0.25 npm/? node/v20.11.1 darwin x64";

    const result = getPkgManager();
    expect(result).toBe("bun");
  });

  it("should detect deno when run via deno", () => {
    process.env.npm_config_user_agent =
      "deno/1.40.0 npm/? node/v20.11.1 darwin x64";

    const result = getPkgManager();
    expect(result).toBe("deno");
  });

  it("should default to npm when no user agent is set", () => {
    delete process.env.npm_config_user_agent;

    const result = getPkgManager();
    expect(result).toBe("npm");
  });

  it("should default to npm when user agent is empty", () => {
    process.env.npm_config_user_agent = "";

    const result = getPkgManager();
    expect(result).toBe("npm");
  });

  it("should default to npm when user agent doesn't match any known package manager", () => {
    process.env.npm_config_user_agent =
      "unknown-manager/1.0.0 npm/? node/v20.11.1 darwin x64";

    const result = getPkgManager();
    expect(result).toBe("npm");
  });
});
