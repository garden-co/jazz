import fs from "fs";
import path from "path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import YAML from "yaml";
import {
  findWorkspaceRoot,
  parseCatalogDefinitions,
  resolveCatalogVersion,
} from "../src/catalog.js";

describe("Catalog Resolution", () => {
  let mockWorkspaceRoot: string;
  let mockWorkspaceFile: string;

  beforeEach(() => {
    // Create a temporary workspace structure for testing
    mockWorkspaceRoot = path.join(__dirname, "mock-workspace");
    mockWorkspaceFile = path.join(mockWorkspaceRoot, "pnpm-workspace.yaml");

    if (!fs.existsSync(mockWorkspaceRoot)) {
      fs.mkdirSync(mockWorkspaceRoot, { recursive: true });
    }
  });

  afterEach(() => {
    // Clean up mock workspace
    if (fs.existsSync(mockWorkspaceRoot)) {
      fs.rmSync(mockWorkspaceRoot, { recursive: true, force: true });
    }
  });

  // Helper function to create a mock workspace file
  function createMockWorkspace(
    catalogs: Record<string, Record<string, string>>,
  ) {
    const workspaceConfig = {
      packages: ["packages/*"],
      catalogs,
    };
    fs.writeFileSync(mockWorkspaceFile, YAML.stringify(workspaceConfig));
  }

  describe("findWorkspaceRoot", () => {
    it("should find workspace root when run from workspace directory", () => {
      createMockWorkspace({});
      const originalCwd = process.cwd();

      try {
        process.chdir(mockWorkspaceRoot);
        const result = findWorkspaceRoot();
        expect(result).toBe(mockWorkspaceRoot);
      } finally {
        process.chdir(originalCwd);
      }
    });

    it("should find workspace root when run from subdirectory", () => {
      createMockWorkspace({});
      const subDir = path.join(mockWorkspaceRoot, "packages", "test");
      fs.mkdirSync(subDir, { recursive: true });
      const originalCwd = process.cwd();

      try {
        process.chdir(subDir);
        const result = findWorkspaceRoot();
        expect(result).toBe(mockWorkspaceRoot);
      } finally {
        process.chdir(originalCwd);
      }
    });

    it("should throw error when no workspace found", () => {
      // Start from filesystem root to ensure no workspace file is found
      const fsRoot = path.parse(process.cwd()).root;
      const originalCwd = process.cwd();

      try {
        process.chdir(fsRoot);
        expect(() => findWorkspaceRoot()).toThrow(
          "Could not find pnpm-workspace.yaml in any parent directory",
        );
      } finally {
        process.chdir(originalCwd);
      }
    });
  });

  describe("parseCatalogDefinitions", () => {
    it("should parse catalog definitions correctly", () => {
      const mockCatalogs = {
        default: {
          typescript: "5.6.2",
          vite: "6.3.5",
        },
        react: {
          react: "19.1.0",
          "react-dom": "19.1.0",
        },
      };

      createMockWorkspace(mockCatalogs);
      const originalCwd = process.cwd();

      try {
        process.chdir(mockWorkspaceRoot);
        const result = parseCatalogDefinitions();
        expect(result).toEqual(mockCatalogs);
      } finally {
        process.chdir(originalCwd);
      }
    });

    it("should return empty object when no catalogs defined", () => {
      createMockWorkspace({});
      const originalCwd = process.cwd();

      try {
        process.chdir(mockWorkspaceRoot);
        const result = parseCatalogDefinitions();
        expect(result).toEqual({});
      } finally {
        process.chdir(originalCwd);
      }
    });

    it("should return empty object when file doesn't exist", () => {
      const originalCwd = process.cwd();
      const fsRoot = path.parse(process.cwd()).root;

      try {
        process.chdir(fsRoot);
        const result = parseCatalogDefinitions();
        expect(result).toEqual({});
      } finally {
        process.chdir(originalCwd);
      }
    });
  });

  describe("resolveCatalogVersion", () => {
    const mockCatalogs = {
      default: {
        typescript: "5.6.2",
        vite: "6.3.5",
        vitest: "3.2.4",
      },
      react: {
        react: "19.1.0",
        "react-dom": "19.1.0",
        "@types/react": "19.1.0",
      },
      rn: {
        "react-native": "0.80.0",
        react: "19.1.0",
      },
    };

    it("should resolve catalog: to default catalog", () => {
      const result = resolveCatalogVersion(
        "typescript",
        "catalog:",
        mockCatalogs,
      );
      expect(result).toBe("5.6.2");
    });

    it("should resolve catalog:default explicitly", () => {
      const result = resolveCatalogVersion(
        "vite",
        "catalog:default",
        mockCatalogs,
      );
      expect(result).toBe("6.3.5");
    });

    it("should resolve specific catalog references", () => {
      const reactResult = resolveCatalogVersion(
        "react",
        "catalog:react",
        mockCatalogs,
      );
      expect(reactResult).toBe("19.1.0");

      const rnResult = resolveCatalogVersion(
        "react-native",
        "catalog:rn",
        mockCatalogs,
      );
      expect(rnResult).toBe("0.80.0");
    });

    it("should return null for non-catalog references", () => {
      const result = resolveCatalogVersion("react", "^19.0.0", mockCatalogs);
      expect(result).toBeNull();
    });

    it("should return null for missing catalog", () => {
      const result = resolveCatalogVersion(
        "react",
        "catalog:nonexistent",
        mockCatalogs,
      );
      expect(result).toBeNull();
    });

    it("should return null for missing package in catalog", () => {
      const result = resolveCatalogVersion(
        "nonexistent",
        "catalog:react",
        mockCatalogs,
      );
      expect(result).toBeNull();
    });

    it("should handle scoped package names", () => {
      const result = resolveCatalogVersion(
        "@types/react",
        "catalog:react",
        mockCatalogs,
      );
      expect(result).toBe("19.1.0");
    });
  });
});
