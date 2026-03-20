import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { getDefaultConfig } = require("expo/metro-config");

const __filename = fileURLToPath(import.meta.url);
const projectRoot = path.dirname(__filename);

const config = getDefaultConfig(projectRoot);

// So Metro uses our Babel config (babel.config.cjs); it doesn't auto-detect .cjs
config.transformer = config.transformer || {};
config.transformer.extendsBabelConfigPath = path.resolve(projectRoot, "babel.config.cjs");

// pnpm uses symlinks for hoisted packages
config.resolver.unstable_enableSymlinks = true;

export default config;
