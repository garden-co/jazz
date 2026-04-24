import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { withJazz } from "jazz-tools/dev/expo";

const require = createRequire(import.meta.url);
const { getDefaultConfig } = require("expo/metro-config");

const __filename = fileURLToPath(import.meta.url);
const projectRoot = path.dirname(__filename);

const config = getDefaultConfig(projectRoot);

// pnpm uses symlinks for hoisted packages
config.resolver.unstable_enableSymlinks = true;

// Start Jazz dev server and inject EXPO_PUBLIC_JAZZ_* env vars for Metro to inline.
// Metro supports async config, so top-level await is fine here.
await withJazz({}, { schemaDir: projectRoot });

export default config;
