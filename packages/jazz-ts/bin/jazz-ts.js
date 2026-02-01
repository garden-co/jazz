#!/usr/bin/env node

import { register } from "tsx/esm/api";

// Register tsx loader so we can import TypeScript files
register();

// Now import and run the CLI
await import("../dist/cli.js");
