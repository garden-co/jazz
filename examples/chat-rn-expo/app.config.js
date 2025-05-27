const { withBuildProperties } = require("expo-build-properties");
const {
  withDangerousMod,
  withGradleProperties,
} = require("@expo/config-plugins");
const fs = require("fs/promises");
const path = require("path");

// This function ensures SODIUM_ENABLED is set in the Podfile
function withSodiumEnabledIos(config) {
  return withDangerousMod(config, [
    "ios",
    async (config) => {
      const podfilePath = path.join(
        config.modRequest.platformProjectRoot,
        "Podfile",
      );
      let contents = await fs.readFile(podfilePath, "utf-8");

      // Check if SODIUM_ENABLED is already set
      if (!contents.includes("ENV['SODIUM_ENABLED']")) {
        // Add it right after the RCT_NEW_ARCH_ENABLED ENV variable
        contents = contents.replace(
          /^(ENV\['RCT_NEW_ARCH_ENABLED'\].*$)/m,
          `$1\nENV['SODIUM_ENABLED'] = '1'`,
        );
        await fs.writeFile(podfilePath, contents);
      }

      return config;
    },
  ]);
}

/**
 *  https://github.com/mrousavy/nitro/issues/422#issuecomment-2545988256
 */
function withCustomIosMod(config) {
  // Use expo-build-properties to bump iOS deployment target
  config = withBuildProperties(config, { ios: { deploymentTarget: "16.0" } });
  // Patch the generated Podfile fallback to ensure platform is always 16.0
  config = withDangerousMod(config, [
    "ios",
    async (modConfig) => {
      const podfilePath = path.join(
        modConfig.modRequest.platformProjectRoot,
        "Podfile",
      );
      let contents = await fs.readFile(podfilePath, "utf-8");

      // Check if the IPHONEOS_DEPLOYMENT_TARGET setting is already present
      // We search for the key being assigned, e.g., config.build_settings['IPHONEOS_DEPLOYMENT_TARGET'] =
      const deploymentTargetSettingExists =
        /\.build_settings\s*\[\s*['"]IPHONEOS_DEPLOYMENT_TARGET['"]\s*\]\s*=/.test(
          contents,
        );

      if (!deploymentTargetSettingExists) {
        // IPHONEOS_DEPLOYMENT_TARGET setting not found, proceed to add it.
        contents = contents.replace(
          /(post_install\s+do\s+\|installer\|[\s\S]*?)(\r?\n\s  end\s*)$/m,
          `$1

    # Expo Build Properties: force deployment target
    # https://github.com/mrousavy/nitro/issues/422#issuecomment-2545988256
    installer.pods_project.targets.each do |target|
      target.build_configurations.each do |config|
        config.build_settings['IPHONEOS_DEPLOYMENT_TARGET'] = '16.0'
      end
    end
$2`,
        );
      }

      await fs.writeFile(podfilePath, contents);
      return modConfig;
    },
  ]);
  return config;
}

// Add Android Gradle properties
function withSodiumEnabledAndroid(config) {
  return withGradleProperties(config, (config) => {
    config.modResults = config.modResults || [];

    // Check if the property already exists
    const existingProperty = config.modResults.find(
      (item) => item.type === "property" && item.key === "sodiumEnabled",
    );

    if (!existingProperty) {
      config.modResults.push({
        type: "property",
        key: "sodiumEnabled",
        value: "true",
      });
    }

    return config;
  });
}

module.exports = ({ config }) => {
  // Apply all configurations
  config = withCustomIosMod(config);
  config = withSodiumEnabledIos(config);
  config = withSodiumEnabledAndroid(config);

  return config;
};
