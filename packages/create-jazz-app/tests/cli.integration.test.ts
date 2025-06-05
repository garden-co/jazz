import fs from "fs";
import path from "path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Mock inquirer to avoid interactive prompts during testing
vi.mock("inquirer", () => ({
  default: {
    prompt: vi.fn(),
  },
}));

// Mock degit to avoid actual cloning during tests
vi.mock("degit", () => ({
  default: vi.fn(() => ({
    clone: vi.fn().mockResolvedValue(undefined),
  })),
}));

// Mock ora (spinner) to avoid output during tests
vi.mock("ora", () => ({
  default: vi.fn(() => ({
    start: vi.fn().mockReturnThis(),
    succeed: vi.fn().mockReturnThis(),
    fail: vi.fn().mockReturnThis(),
  })),
}));

describe("CLI Integration Tests", () => {
  const testDir = path.join(__dirname, "..", "test-temp");
  let originalUserAgent: string | undefined;
  let originalCwd: string;

  beforeEach(() => {
    originalUserAgent = process.env.npm_config_user_agent;
    originalCwd = process.cwd();

    // Create test directory
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true, force: true });
    }
    fs.mkdirSync(testDir, { recursive: true });
  });

  afterEach(() => {
    // Restore original values
    if (originalUserAgent !== undefined) {
      process.env.npm_config_user_agent = originalUserAgent;
    } else {
      delete process.env.npm_config_user_agent;
    }

    process.chdir(originalCwd);

    // Clean up test directory
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true, force: true });
    }
  });

  it("should automatically select pnpm when invoked via pnpm dlx", async () => {
    // Set up environment to simulate pnpm dlx
    process.env.npm_config_user_agent =
      "pnpm/8.15.4 npm/? node/v20.11.1 darwin x64";

    // Mock inquirer.prompt to simulate user selecting all other options except package manager
    const inquirer = await import("inquirer");
    const mockPrompt = vi.mocked(inquirer.default.prompt);

    mockPrompt.mockResolvedValue({
      starter: "react-demo-auth",
      projectName: "test-app",
      // Note: packageManager is not included, so it should be auto-detected
    });

    // We can't easily test the full CLI without actually running it,
    // but we can test that the package manager detection works correctly
    // by importing and testing the logic directly
    const { getPkgManager } = await import("../src/utils.js");

    const detectedManager = getPkgManager();
    expect(detectedManager).toBe("pnpm");
  });

  it("should automatically select yarn when invoked via yarn", async () => {
    process.env.npm_config_user_agent =
      "yarn/1.22.19 npm/? node/v20.11.1 darwin x64";

    const { getPkgManager } = await import("../src/utils.js");
    const detectedManager = getPkgManager();
    expect(detectedManager).toBe("yarn");
  });

  it("should fall back to npm when no user agent is set", async () => {
    delete process.env.npm_config_user_agent;

    const { getPkgManager } = await import("../src/utils.js");
    const detectedManager = getPkgManager();
    expect(detectedManager).toBe("npm");
  });
});
