import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { getDefaultConfig } = require("expo/metro-config");
const config = getDefaultConfig(path.dirname(fileURLToPath(import.meta.url)));
config.resolver.unstable_enableSymlinks = true;
config.resolver.assetExts = [...config.resolver.assetExts, "wasm"];
export default config;
