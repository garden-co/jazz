import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { getDefaultConfig } = require("expo/metro-config");

const __filename = fileURLToPath(import.meta.url);
const projectRoot = path.dirname(__filename);
const workspaceRoot = path.resolve(projectRoot, "../..");

const config = getDefaultConfig(projectRoot);

// So Metro uses our Babel config (babel.config.cjs); it doesn't auto-detect .cjs
config.transformer = config.transformer || {};
config.transformer.extendsBabelConfigPath = path.resolve(projectRoot, "babel.config.cjs");

config.watchFolders = [workspaceRoot];
config.resolver.nodeModulesPaths = [
  path.resolve(projectRoot, "node_modules"),
  path.resolve(workspaceRoot, "node_modules"),
];
config.resolver.extraNodeModules = {
  ...config.resolver.extraNodeModules,
  react: path.resolve(projectRoot, "node_modules/react"),
  "react-native": path.resolve(projectRoot, "node_modules/react-native"),
};
config.resolver.unstable_enableSymlinks = true;
config.resolver.unstable_enablePackageExports = true;

export default config;
