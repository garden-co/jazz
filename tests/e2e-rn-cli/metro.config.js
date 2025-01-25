const { getDefaultConfig, mergeConfig } = require("@react-native/metro-config");
const path = require("path");
const escape = require("escape-string-regexp");
const exclusionList = require("metro-config/src/defaults/exclusionList");

const root = path.resolve(__dirname, "../..");
const modules = ["react-native"];

/**
 * Metro configuration
 * https://reactnative.dev/docs/metro
 *
 * @type {import("@react-native/metro-config").MetroConfig}
 */
const config = {
  watchFolders: [root],

  // We need to make sure that only one version is loaded for `modules` (monorepo stuffs)
  // So we block them at the root, and alias them to the versions in this app"s node_modules
  resolver: {
    blacklistRE: exclusionList(
      modules.map(
        (m) =>
          new RegExp(`^${escape(path.join(root, "node_modules", m))}\\/.*$`),
      ),
    ),

    extraNodeModules: modules.reduce((acc, name) => {
      acc[name] = path.join(__dirname, "node_modules", name);
      return acc;
    }, {}),
    unstable_enablePackageExports: true,
  },

  transformer: {
    getTransformOptions: async () => ({
      transform: {
        experimentalImportSupport: false,
        inlineRequires: true,
      },
    }),
  },
};

module.exports = mergeConfig(getDefaultConfig(__dirname), config);
