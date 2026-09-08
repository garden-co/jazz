/* Expo prebuild hook: injects the test-only trusted fixture source into the
 * generated host. Account admission stays in the shared production path. */
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
    },
  );
  // The label-gated iOS simulator workflow registers this fixture after
  // prebuild, stages the pod/XCFramework, and requires its linked
  // ABI/admission receipt. Multi-peer acceptance remains TODO (#2291).
  return copyTemplate(config, "ios", "ios/JazzDeviceFixture.mm", "JazzDeviceFixture.mm");
};
