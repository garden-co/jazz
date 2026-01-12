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
      // Force react-native-passkey to resolve from workspace root
      if (moduleName === "react-native-passkey") {
        return {
          type: "sourceFile",
          filePath: path.resolve(
            workspaceRoot,
            "node_modules/react-native-passkey/lib/module/index.js",
          ),
        };
      }

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
    extraNodeModules: {
      "react-native-passkey": path.resolve(
        workspaceRoot,
        "node_modules/react-native-passkey",
      ),
    },
    sourceExts: ["mjs", "js", "json", "ts", "tsx"],
    unstable_enableSymlinks: true,
  },
});
