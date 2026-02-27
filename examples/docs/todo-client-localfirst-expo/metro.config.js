const path = require("node:path");
const fs = require("node:fs");
const { getDefaultConfig } = require("expo/metro-config");

const projectRoot = __dirname;
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

// Resolve relative "./foo.js" to "./foo.ts" or "./foo.tsx" when .js doesn't exist (TS source in workspace)
const defaultResolveRequest = config.resolver.resolveRequest;
config.resolver.resolveRequest = (context, moduleName, platform) => {
  if (moduleName.startsWith(".") && moduleName.endsWith(".js") && context.originModulePath) {
    const dir = path.dirname(context.originModulePath);
    const base = path.join(dir, moduleName.slice(0, -3));
    for (const ext of [".ts", ".tsx", ".js", ".jsx"]) {
      const candidate = base + ext;
      try {
        if (fs.existsSync(candidate)) {
          return { type: "sourceFile", filePath: candidate };
        }
      } catch (_) {}
    }
  }
  return defaultResolveRequest ? defaultResolveRequest(context, moduleName, platform) : null;
};

module.exports = config;
