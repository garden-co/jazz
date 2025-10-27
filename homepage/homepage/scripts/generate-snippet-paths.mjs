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
const SNIPPETS_TSCONFIG_PATH = path.join(SNIPPETS_DIR, "tsconfig.json");

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
  const mappings = {};

  if (!fs.existsSync(SNIPPETS_DIR)) {
    console.warn("Snippets directory not found:", SNIPPETS_DIR);
    return mappings;
  }

  const snippetDirs = fs
    .readdirSync(SNIPPETS_DIR)
    .filter((name) => fs.statSync(path.join(SNIPPETS_DIR, name)).isDirectory());

  for (const dirName of snippetDirs) {
    const aliases = findAliasesInDir(path.join(SNIPPETS_DIR, dirName));
    verbose && console.log(`Found ${aliases.size} aliases in ${dirName}`);

    for (const alias of aliases) {
      mappings[alias] ??= new Set();
      mappings[alias].add(`./content/docs/code-snippets/${dirName}/*`);
    }
  }

  return mappings;
}

function stripJsonComments(text) {
  return text.replace(/\/\/.*$/gm, "");
}

function updateRootTsconfig(pathMappings) {
  const config = JSON.parse(
    stripJsonComments(fs.readFileSync(TSCONFIG_PATH, "utf-8")),
  );
  config.compilerOptions ??= {};
  config.compilerOptions.paths = Object.fromEntries(
    Object.entries(pathMappings).map(([k, v]) => [k, [...v]]),
  );
  config.exclude = ["node_modules"];
  fs.writeFileSync(TSCONFIG_PATH, JSON.stringify(config, null, 2) + "\n");
}

function writeSnippetsTsconfig(pathMappings) {
  const relativePaths = Object.fromEntries(
    Object.entries(pathMappings).map(([alias, paths]) => [
      alias,
      [...paths].map((p) => p.replace("./content/docs/code-snippets/", "./")),
    ]),
  );

  const snippetsConfig = {
    extends: "../../../tsconfig.snippets.json",
    compilerOptions: { baseUrl: ".", paths: relativePaths },
  };

  fs.writeFileSync(
    SNIPPETS_TSCONFIG_PATH,
    JSON.stringify(snippetsConfig, null, 2) + "\n",
  );
  verbose && console.log("Wrote", SNIPPETS_TSCONFIG_PATH);
}

console.log("Generating TypeScript path mappings for code snippets...");
const mappings = collectPathMappings();
updateRootTsconfig(mappings);
writeSnippetsTsconfig(mappings);
console.log("✅ Done!");
