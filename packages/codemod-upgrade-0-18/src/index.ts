#!/usr/bin/env node

import { runTransform } from "./transform.js";

const args = process.argv.slice(2);
const projectPath = args[0] || process.cwd();

console.log(`Running Jazz API migration transform on: ${projectPath}`);
runTransform(projectPath);
console.log("Transform completed!");
