/**
 * Keep the package's relay-boundary receipts runnable in the same Babel mode
 * as the React Native host. Without this local config Jest loads RN's ESM
 * setup file untransformed, so missing/incompatible native-build diagnostics
 * silently have no executable coverage.
 */
module.exports = {
  presets: ["module:@react-native/babel-preset"],
};
