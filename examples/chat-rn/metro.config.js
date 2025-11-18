const path = require("path");
const { makeMetroConfig } = require("@rnx-kit/metro-config");
const MetroSymlinksResolver = require("@rnx-kit/metro-resolver-symlinks");

const projectRoot = __dirname;
const workspaceRoot = path.resolve(projectRoot, "../..");

// Custom resolver to handle the problematic dynamic import
const customResolver = (context, moduleName, platform) => {
  if (moduleName.includes("@bam.tech/react-native-image-resizer")) {
    const packagePath = path.resolve(
      workspaceRoot,
      "node_modules/@bam.tech/react-native-image-resizer",
    );

    // Always point to the actual file location
    return {
      type: "sourceFile",
      filePath: path.join(packagePath, "lib/module/index.js"),
    };
  }

  // First try the symlinks resolver
  const symlinkResolver = MetroSymlinksResolver();
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
};

module.exports = makeMetroConfig({
  projectRoot,
  watchFolders: [
    path.resolve(workspaceRoot, "node_modules"),
    path.resolve(workspaceRoot, "packages"),
  ],
  resolver: {
    resolveRequest: customResolver,
    nodeModulesPaths: [
      path.resolve(projectRoot, "node_modules"),
      path.resolve(workspaceRoot, "node_modules"),
    ],
    sourceExts: ["mjs", "js", "json", "ts", "tsx"],
    unstable_enableSymlinks: true,
  },
});
