const path = require("path");
const { makeMetroConfig } = require("@rnx-kit/metro-config");
const MetroSymlinksResolver = require("@rnx-kit/metro-resolver-symlinks");

const projectRoot = __dirname;
const workspaceRoot = path.resolve(projectRoot, "../..");

const symlinkResolver = MetroSymlinksResolver();

module.exports = makeMetroConfig({
  projectRoot,
  watchFolders: [
    path.resolve(workspaceRoot, "node_modules"),
    path.resolve(workspaceRoot, "packages"),
    path.resolve(workspaceRoot, "crates"),
  ],
  resolver: {
    resolveRequest: (context, moduleName, platform) => {
      // First try the symlinks resolver
      try {
        const result = symlinkResolver(context, moduleName, platform);
        if (result) {
          return result;
        }
      } catch (e) {
        // Continue to fallback
      }

      // Fall back to default resolution
      return context.resolveRequest(context, moduleName, platform);
    },
    nodeModulesPaths: [
      path.resolve(projectRoot, "node_modules"),
      path.resolve(workspaceRoot, "node_modules"),
    ],
    sourceExts: ["mjs", "js", "json", "ts", "tsx"],
    unstable_enableSymlinks: true,
  },
});
