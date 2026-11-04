// CLI only; the logic and its tests live in dev/artifacts/native-inputs.mjs.
// build:runtime bundles the jazz-wasm package and the generated native
// fingerprint modules, so this builds and stages whichever are missing.
import { ensureNativeInputs } from "../../../dev/artifacts/native-inputs.mjs";

try {
  ensureNativeInputs();
} catch (error) {
  console.error(`jazz-tools native inputs: ${error.message}`);
  process.exitCode = 1;
}
