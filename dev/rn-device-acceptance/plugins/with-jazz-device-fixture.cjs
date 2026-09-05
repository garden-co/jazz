/* Expo prebuild hook: injects the test-only trusted fixture source into the
 * generated host. It intentionally does not expose configuration to JS. */
const { withAndroidManifest, withDangerousMod } = require("@expo/config-plugins");
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
  if (source.includes("JAZZ_DEVICE_APP_ID")) return;
  const marker =
    'buildConfigField "String", "REACT_NATIVE_RELEASE_LEVEL", "\\"${findProperty(\'reactNativeReleaseLevel\') ?: \'stable\'}\\""';
  const fields = [
    "        // Schema/app metadata is public; endpoint and ephemeral bearers are",
    "        // launch-only inputs from the local Rust Edge/Core harness.",
    '        buildConfigField "String", "JAZZ_DEVICE_APP_ID", "\\"jazz-device-acceptance\\""',
    `        buildConfigField "String", "JAZZ_DEVICE_SCHEMA_JSON", ${JSON.stringify(JSON.stringify(JSON.stringify(require("../native/device-fixture.json").schema)))}`, // todos and policy-protected scope_rows
  ].join("\n");
  if (!source.includes(marker))
    throw new Error("Expo app build.gradle no longer has the BuildConfig insertion marker");
  fs.writeFileSync(buildGradle, source.replace(marker, `${marker}\n${fields}`));
}

module.exports = function withJazzDeviceFixture(config) {
  config = withAndroidManifest(config, (mod) => {
    const application = mod.modResults.manifest.application?.[0];
    if (!application) throw new Error("Expo Android manifest has no application");
    application.$["android:networkSecurityConfig"] = "@xml/jazz_device_network_security";
    return mod;
  });
  config = copyTemplate(
    config,
    "android",
    "android/jazz_device_network_security.xml",
    "app/src/main/res/xml/jazz_device_network_security.xml",
  );
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
  // The label-gated iOS simulator workflow registers this fixture after
  // prebuild, stages the pod/XCFramework, and requires its linked
  // ABI/admission receipt. Multi-peer acceptance remains TODO (#2291).
  return copyTemplate(config, "ios", "ios/JazzDeviceFixture.mm", "JazzDeviceFixture.mm");
};
