/* Expo prebuild hook: injects the test-only trusted fixture source into the
 * generated host. It intentionally does not expose configuration to JS. */
const { withDangerousMod } = require("@expo/config-plugins");
const fs = require("node:fs");
const path = require("node:path");

function copyTemplate(config, platform, source, destination) {
  return withDangerousMod(config, [platform, async (mod) => {
    const target = path.join(mod.modRequest.platformProjectRoot, destination);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.copyFileSync(path.join(__dirname, "..", "native", source), target);
    return mod;
  }]);
}

module.exports = function withJazzDeviceFixture(config) {
  // Registration is intentionally TODO: Expo's generated Android/iOS host
  // shapes differ by SDK. Keeping templates injected but unregistered makes
  // prebuild deterministic without claiming a compiled fixture receipt.
  config = copyTemplate(config, "android", "android/JazzDeviceFixtureModule.kt", "app/src/main/java/dev/jazz/rndeviceacceptance/JazzDeviceFixtureModule.kt");
  return copyTemplate(config, "ios", "ios/JazzDeviceFixture.mm", "JazzDeviceFixture.mm");
};
