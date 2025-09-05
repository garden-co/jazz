const path = require("path");
const { getDefaultConfig } = require("expo/metro-config");

const projectRoot = path.resolve(__dirname);
const workspaceRoot = path.resolve(projectRoot, "../..");

const config = getDefaultConfig(projectRoot);

config.watchFolders = [
  workspaceRoot,
  // required for this monorepo, but not for a regular app
  path.resolve(workspaceRoot, "crates", "cojson-core-rn", "pkg"),
];

module.exports = config;
