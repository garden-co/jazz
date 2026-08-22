/**
 * Expo config plugin for jazz-rn.
 *
 * JazzRn is a TurboModule backed by Rust/JSI.  Expo Go cannot load arbitrary
 * native modules, so consumers must create a development build.  The plugin's
 * only native configuration is enabling React Native's new architecture,
 * which is required for this TurboModule and is safe to apply repeatedly.
 */
module.exports = function withJazzRn(config) {
  return {
    ...config,
    newArchEnabled: true,
  };
};
