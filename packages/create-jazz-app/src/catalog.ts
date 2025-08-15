import fs from "fs";
import path from "path";
import chalk from "chalk";
import YAML from "yaml";

type CatalogDefinitions = {
  [catalogName: string]: Record<string, string>;
};

export function findWorkspaceRoot(): string {
  let currentDir = process.cwd();

  while (currentDir !== path.dirname(currentDir)) {
    const workspaceFile = path.join(currentDir, "pnpm-workspace.yaml");
    if (fs.existsSync(workspaceFile)) {
      return currentDir;
    }
    currentDir = path.dirname(currentDir);
  }

  throw new Error("Could not find pnpm-workspace.yaml in any parent directory");
}

export function parseCatalogDefinitions(): CatalogDefinitions {
  try {
    const workspaceRoot = findWorkspaceRoot();
    const workspaceFile = path.join(workspaceRoot, "pnpm-workspace.yaml");
    const workspaceContent = fs.readFileSync(workspaceFile, "utf8");
    const workspaceConfig = YAML.parse(workspaceContent);

    return workspaceConfig.catalogs || {};
  } catch (error) {
    console.warn(
      chalk.yellow(
        "Warning: Could not parse pnpm-workspace.yaml, catalog dependencies will not be resolved",
      ),
    );
    return {};
  }
}

export function resolveCatalogVersion(
  packageName: string,
  catalogRef: string,
  catalogs: CatalogDefinitions,
): string | null {
  // Parse catalog reference: "catalog:" uses default, "catalog:react" uses react catalog
  const catalogName = catalogRef.startsWith("catalog:")
    ? catalogRef.slice(8) || "default"
    : null;

  if (!catalogName) return null;

  const catalog = catalogs[catalogName];
  if (!catalog) {
    console.warn(
      chalk.yellow(
        `Warning: Catalog "${catalogName}" not found in pnpm-workspace.yaml`,
      ),
    );
    return null;
  }

  const version = catalog[packageName];
  if (!version) {
    console.warn(
      chalk.yellow(
        `Warning: Package "${packageName}" not found in catalog "${catalogName}"`,
      ),
    );
    return null;
  }

  return version;
}
