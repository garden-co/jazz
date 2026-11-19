const { getDefaultConfig } = require("expo/metro-config");
const { withJazz } = require("jazz-tools/expo/metro");
module.exports = withJazz(getDefaultConfig(__dirname));
