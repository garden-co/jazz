#!/usr/bin/env node

import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

function findPackageJsonFiles(dir) {
  const files = readdirSync(dir, { withFileTypes: true });
  let packageJsonFiles = [];

  for (const file of files) {
    const fullPath = join(dir, file.name);
    if (
      file.isDirectory() &&
      !file.name.startsWith(".") &&
      file.name !== "node_modules"
    ) {
      packageJsonFiles = packageJsonFiles.concat(
        findPackageJsonFiles(fullPath),
      );
    } else if (file.name === "package.json") {
      packageJsonFiles.push(fullPath);
    }
  }

  return packageJsonFiles;
}

function checkCatalogDependencies(packageJsonPath) {
  const content = readFileSync(packageJsonPath, "utf-8");
  const packageJson = JSON.parse(content);
  const issues = [];

  function checkDeps(deps, type) {
    if (!deps) return;
    for (const [pkg, version] of Object.entries(deps)) {
      if (version === "catalog:") {
        issues.push({
          package: pkg,
          type,
          path: packageJsonPath,
        });
      }
    }
  }

  checkDeps(packageJson.dependencies, "dependencies");
  checkDeps(packageJson.peerDependencies, "peerDependencies");

  return issues;
}

const rootDir = process.cwd();
const packageJsonFiles = findPackageJsonFiles(rootDir);
let hasIssues = false;

for (const file of packageJsonFiles) {
  const issues = checkCatalogDependencies(file);
  if (issues.length > 0) {
    hasIssues = true;
    console.log(`\nIssues found in ${file}:`);
    for (const issue of issues) {
      console.log(
        `  - ${issue.package} in ${issue.type} uses 'catalog:' version`,
      );
    }
  }
}

if (hasIssues) {
  console.log(
    '\nError: Found packages using "catalog:" as version. Please use specific versions instead.',
  );
  process.exit(1);
} else {
  console.log('No issues found with "catalog:" dependencies.');
  process.exit(0);
}
