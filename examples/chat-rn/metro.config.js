const { getDefaultConfig, mergeConfig } = require("@react-native/metro-config");
const path = require("path");

// eslint-disable-next-line no-undef
const projectRoot = __dirname;
const workspaceRoot = path.resolve(projectRoot, "../..");

/**
 * Metro configuration
 * https://reactnative.dev/docs/metro
 *
 * @type {import('@react-native/metro-config').MetroConfig}
 */
const config = {
  resolver: {
    nodeModulesPaths: [
      path.resolve(projectRoot, "node_modules"),
      path.resolve(workspaceRoot, "node_modules"),
    ],
    sourceExts: ["mjs", "js", "json", "ts", "tsx"],
  },
  watchFolders: [workspaceRoot],
};

module.exports = mergeConfig(getDefaultConfig(__dirname), config);
