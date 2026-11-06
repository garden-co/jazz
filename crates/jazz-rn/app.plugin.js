/**
 * Expo config plugin for jazz-rn.
 *
 * Jazz's relay is a TurboModule. Expo Go cannot load arbitrary native modules,
 * so consumers must create a development build. The plugin's only native
 * configuration restricts Android builds to the ABI slices sealed in the
 * package. It is safe to apply repeatedly and does not claim that a Rust relay
 * artifact is embedded.
 *
 * The relay needs React Native's New Architecture, but the plugin does not
 * set the top-level `newArchEnabled` field: Expo SDK 54 already defaults to
 * the New Architecture, and SDK 55+ has no legacy architecture and rejects the
 * field in its config schema (expo-doctor fails). An app that opts out on SDK
 * 54 gets an explicit error from JazzRn.podspec and android/build.gradle.
 */
const { withGradleProperties } = require("expo/config-plugins");

const androidRelayArchitectures = "armeabi-v7a,arm64-v8a,x86_64";

module.exports = function withJazzRn(config) {
  return withGradleProperties(config, (config) => {
    const property = config.modResults.find(
      (item) => item.type === "property" && item.key === "reactNativeArchitectures",
    );
    if (property) property.value = androidRelayArchitectures;
    else
      config.modResults.push({
        type: "property",
        key: "reactNativeArchitectures",
        value: androidRelayArchitectures,
      });
    return config;
  });
};
