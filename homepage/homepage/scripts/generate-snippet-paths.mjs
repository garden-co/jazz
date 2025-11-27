#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const verbose = false;

const ROOT = path.resolve(__dirname, "..");
const SNIPPETS_DIR = path.join(ROOT, "content/docs/code-snippets");
const TSCONFIG_PATH = path.join(ROOT, "tsconfig.snippets.json");

function extractAliasImports(source) {
  const regex = /(?:from\s+['"]|import\s*\(\s*['"])([^'"]+)['"]/g;
  const results = [];
  let match;
  while ((match = regex.exec(source))) {
    const imp = match[1];
    if (/^[@$~]/.test(imp)) results.push(imp);
  }
  return results;
}

function expandAliasPatterns(importPath) {
  const parts = importPath.split("/");
  const patterns = [];
  for (let i = parts.length - 1; i > 0; i--) {
    patterns.push(parts.slice(0, i).join("/") + "/*");
  }
  return patterns;
}

function findAliasesInDir(dirPath) {
  const aliases = new Set();

  for (const file of fs.readdirSync(dirPath)) {
    const fullPath = path.join(dirPath, file);
    const stat = fs.statSync(fullPath);

    if (stat.isDirectory()) continue;
    if (!/\.(ts|tsx|js|jsx|svelte)$/.test(file)) continue;

    const content = fs.readFileSync(fullPath, "utf-8");
    for (const imp of extractAliasImports(content)) {
      if (imp.startsWith("$env/") || imp.startsWith("$app/")) continue;
      for (const pattern of expandAliasPatterns(imp)) {
        aliases.add(pattern);
      }
    }
  }

  return aliases;
}

function collectPathMappings() {
  const mappingsByDir = {};
  const allSnippetDirs = new Set();

  if (!fs.existsSync(SNIPPETS_DIR)) {
    console.warn("Snippets directory not found:", SNIPPETS_DIR);
    return { mappingsByDir, allSnippetDirs };
  }

  // Recursively find all directories containing code files
  function findCodeDirs(dir, relativePath = "") {
    const entries = fs.readdirSync(dir);
    let hasCodeFiles = false;

    for (const entry of entries) {
      const fullPath = path.join(dir, entry);
      const stat = fs.statSync(fullPath);

      if (stat.isDirectory()) {
        const newRelative = relativePath ? `${relativePath}/${entry}` : entry;
        findCodeDirs(fullPath, newRelative);
      } else if (/\.(ts|tsx|js|jsx|svelte)$/.test(entry)) {
        hasCodeFiles = true;
      }
    }

    if (hasCodeFiles && relativePath) {
      allSnippetDirs.add(relativePath);
      const aliases = findAliasesInDir(dir);
      if (aliases.size > 0) {
        mappingsByDir[relativePath] = aliases;
        verbose &&
          console.log(`Found ${aliases.size} aliases in ${relativePath}`);
      }
    }
  }

  findCodeDirs(SNIPPETS_DIR);
  return { mappingsByDir, allSnippetDirs };
}

function stripJsonComments(text) {
  return text.replace(/\/\/.*$/gm, "");
}

function updateRootTsconfig(mappingsByDir) {
  const config = JSON.parse(
    stripJsonComments(fs.readFileSync(TSCONFIG_PATH, "utf-8")),
  );

  // Aggregate all aliases across all directories for the root config
  const allPaths = {};
  for (const [dirName, aliases] of Object.entries(mappingsByDir)) {
    for (const alias of aliases) {
      allPaths[alias] ??= [];
      allPaths[alias].push(`./content/docs/code-snippets/${dirName}/*`);
    }
  }

  config.compilerOptions ??= {};
  config.compilerOptions.paths = allPaths;
  config.exclude = ["node_modules"];
  fs.writeFileSync(TSCONFIG_PATH, JSON.stringify(config, null, 2) + "\n");
}

function writeLocalTsconfigs(mappingsByDir, allSnippetDirs) {
  // Write tsconfig for every snippet directory (not just ones with aliases)
  for (const dirName of allSnippetDirs) {
    const aliases = mappingsByDir[dirName] || new Set();

    // Each directory gets its own tsconfig with local path mappings
    const localPaths = Object.fromEntries(
      [...aliases].map((alias) => [alias, ["./*"]]),
    );

    // Calculate relative depth to root tsconfig
    const depth = dirName.split("/").length;
    const relativeRoot = "../".repeat(depth + 3); // +3 for content/docs/code-snippets

    const localConfig = {
      extends: `${relativeRoot}tsconfig.json`,
      compilerOptions: {
        baseUrl: ".",
        paths: Object.keys(localPaths).length > 0 ? localPaths : undefined,
        target: "ES2020",
        module: "ESNext",
        lib: ["ES2020", "DOM"],
        downlevelIteration: true,
      },
      include: ["./**/*"],
      exclude: [],
    };

    // Remove undefined values
    if (!localConfig.compilerOptions.paths) {
      delete localConfig.compilerOptions.paths;
    }

    const tsconfigPath = path.join(SNIPPETS_DIR, dirName, "tsconfig.json");
    fs.writeFileSync(tsconfigPath, JSON.stringify(localConfig, null, 2) + "\n");
    verbose && console.log(`Wrote ${tsconfigPath}`);
  }
}

console.log("Generating TypeScript path mappings for code snippets...");
const { mappingsByDir, allSnippetDirs } = collectPathMappings();
updateRootTsconfig(mappingsByDir);
writeLocalTsconfigs(mappingsByDir, allSnippetDirs);
console.log("✅ Done!");
