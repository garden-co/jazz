/**
 * Expo config plugin for jazz-rn.
 *
 * Jazz's relay is a TurboModule. Expo Go cannot load arbitrary native modules,
 * so consumers must create a development build. The plugin's only native
 * configuration is enabling React Native's new architecture; it is safe to
 * apply repeatedly and does not claim that a Rust relay artifact is embedded.
 */
module.exports = function withJazzRn(config) {
  return {
    ...config,
    newArchEnabled: true,
  };
};
