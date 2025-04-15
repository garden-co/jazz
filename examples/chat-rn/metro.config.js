const path = require("path");
const { makeMetroConfig } = require("@rnx-kit/metro-config");
const MetroSymlinksResolver = require("@rnx-kit/metro-resolver-symlinks");

// Define workspace root
const projectRoot = __dirname;
const workspaceRoot = path.resolve(projectRoot, "../..");

// Add packages paths
const extraNodeModules = {
  modules: path.resolve(workspaceRoot, "node_modules"),
};

const watchFolders = [
  path.resolve(workspaceRoot, "node_modules"),
  path.resolve(workspaceRoot, "packages"),
];

const nodeModulesPaths = [
  path.resolve(projectRoot, "node_modules"),
  path.resolve(workspaceRoot, "node_modules"),
];

module.exports = makeMetroConfig({
  resolver: {
    resolveRequest: MetroSymlinksResolver(),
    extraNodeModules,
    nodeModulesPaths,
  },
  sourceExts: ["mjs", "js", "json", "ts", "tsx"],
  watchFolders,
});
