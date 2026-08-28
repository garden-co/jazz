/* Expo prebuild hook: injects the test-only trusted fixture source into the
 * generated host. It intentionally does not expose configuration to JS. */
const { withDangerousMod } = require("@expo/config-plugins");
const fs = require("node:fs");
const path = require("node:path");

function copyTemplate(config, platform, source, destination, afterCopy) {
  return withDangerousMod(config, [
    platform,
    async (mod) => {
      const target = path.join(mod.modRequest.platformProjectRoot, destination);
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.copyFileSync(path.join(__dirname, "..", "native", source), target);
      if (afterCopy) afterCopy(mod.modRequest.platformProjectRoot);
      return mod;
    },
  ]);
}

function injectAndroidBuildConfig(root) {
  const buildGradle = path.join(root, "app/build.gradle");
  const source = fs.readFileSync(buildGradle, "utf8");
  if (source.includes("JAZZ_DEVICE_APP_NAMESPACE")) return;
  const marker =
    'buildConfigField "String", "REACT_NATIVE_RELEASE_LEVEL", "\\"${findProperty(\'reactNativeReleaseLevel\') ?: \'stable\'}\\""';
  const fields = [
    "        // Public non-secret compile fixtures; a device job supplies only test material.",
    '        buildConfigField "String", "JAZZ_DEVICE_APP_NAMESPACE", "\\"jazz-device-acceptance\\""',
    '        buildConfigField "String", "JAZZ_DEVICE_STORAGE_NAMESPACE", "\\"acceptance-fixture\\""',
    '        buildConfigField "String", "JAZZ_DEVICE_AUTH_SCOPE", "\\"fixture-user-a\\""',
    '        buildConfigField "String", "JAZZ_DEVICE_SCHEMA_JSON", "\\"{}\\""',
    '        buildConfigField "String", "JAZZ_DEVICE_VERIFIED_IDENTITY_JSON", "\\"{}\\""',
    '        buildConfigField "String", "JAZZ_DEVICE_VERIFIED_CLAIMS_JSON", "\\"{}\\""',
  ].join("\n");
  if (!source.includes(marker))
    throw new Error("Expo app build.gradle no longer has the BuildConfig insertion marker");
  fs.writeFileSync(buildGradle, source.replace(marker, `${marker}\n${fields}`));
}

module.exports = function withJazzDeviceFixture(config) {
  const androidSource = "app/src/main/java/dev/jazz/rndeviceacceptance/";
  config = copyTemplate(
    config,
    "android",
    "android/JazzDeviceFixtureModule.kt",
    `${androidSource}JazzDeviceFixtureModule.kt`,
  );
  config = copyTemplate(
    config,
    "android",
    "android/JazzDeviceFixturePackage.kt",
    `${androidSource}JazzDeviceFixturePackage.kt`,
    (root) => {
      const mainApplication = path.join(root, `${androidSource}MainApplication.kt`);
      const source = fs.readFileSync(mainApplication, "utf8");
      const marker = "// add(MyReactNativePackage())";
      if (!source.includes("add(JazzDeviceFixturePackage())")) {
        if (!source.includes(marker))
          throw new Error("Expo MainApplication template no longer has the fixture package marker");
        fs.writeFileSync(
          mainApplication,
          source.replace(marker, `${marker}\n              add(JazzDeviceFixturePackage())`),
        );
      }
      injectAndroidBuildConfig(root);
    },
  );
  // iOS registration remains TODO until the staged pod/XCFramework compile job
  // verifies its generated-host shape; this workflow is source-only.
  return copyTemplate(config, "ios", "ios/JazzDeviceFixture.mm", "JazzDeviceFixture.mm");
};
