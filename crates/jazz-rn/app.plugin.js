/**
 * Expo config plugin for jazz-rn.
 *
 * Jazz's relay is a TurboModule. Expo Go cannot load arbitrary native modules,
 * so consumers must create a development build. The plugin's only native
 * configuration enables React Native's new architecture and restricts Android
 * builds to the ABI slices sealed in the package. It is safe to apply
 * repeatedly and does not claim that a Rust relay artifact is embedded.
 */
const { withGradleProperties } = require("expo/config-plugins");

const androidRelayArchitectures = "armeabi-v7a,arm64-v8a,x86_64";

module.exports = function withJazzRn(config) {
  return withGradleProperties(
    {
      ...config,
      newArchEnabled: true,
    },
    (config) => {
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
    },
  );
};
