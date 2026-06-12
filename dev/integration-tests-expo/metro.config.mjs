import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { getDefaultConfig } = require("expo/metro-config");

const projectRoot = path.dirname(fileURLToPath(import.meta.url));

const config = getDefaultConfig(projectRoot);

// pnpm uses symlinks for hoisted packages.
config.resolver.unstable_enableSymlinks = true;

// No Jazz dev server: these integration tests run fully local (no serverUrl),
// so we deliberately omit `withJazz` from the Metro config.
export default config;
