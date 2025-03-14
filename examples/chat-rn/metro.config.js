const { getDefaultConfig, mergeConfig } = require("@react-native/metro-config");
// const blacklist = require("metro-config/src/defaults/exclusionList");
const escape = require("escape-string-regexp");
const path = require("path");

// eslint-disable-next-line no-undef
const projectRoot = __dirname;
const workspaceRoot = path.resolve(projectRoot, "../..");
// const dupeDeps = ["react", "react-native"];

/**
 * Metro configuration
 * https://reactnative.dev/docs/metro
 *
 * @type {import('@react-native/metro-config').MetroConfig}
 */
const config = {
  projectRoot,
  resolver: {
    // monorepo node_modules paths
    nodeModulesPaths: [
      path.resolve(projectRoot, "node_modules"),
      path.resolve(workspaceRoot, "node_modules"),
    ],
    // // ensure only one version is loaded for dupeDeps (not the one in workspaceRoot)
    // blacklistRE: blacklist(
    //   dupeDeps.map(
    //     (dep) =>
    //       new RegExp(
    //         `^${escape(path.join(workspaceRoot, "node_modules", dep))}$`,
    //       ),
    //   ),
    // ),
    // extensions
    sourceExts: ["mjs", "js", "json", "ts", "tsx"],
  },
  watchFolders: [workspaceRoot],
};

module.exports = mergeConfig(getDefaultConfig(__dirname), config);
