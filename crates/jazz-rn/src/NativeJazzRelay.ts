import type { TurboModule } from "react-native";
import { TurboModuleRegistry } from "react-native";

/**
 * The future native-relay TurboModule boundary.
 *
 * Commands and responses are opaque base64-encoded canonical Jazz binary
 * payloads. Keeping this surface to one ABI probe and one command prevents
 * React Native from becoming a second query/storage API.
 */
export interface Spec extends TurboModule {
  getAbiVersion(): number;
  installForegroundRuntime(): void;
  execute(commandBase64: string): Promise<string>;
}

// `get`, rather than `getEnforcing`, lets the package explain that the current
// legacy artifact does not yet contain this module. Expo Go and old native
// development builds therefore fail explicitly without pretending JS can add
// the embedded Rust relay through an OTA update.
export default TurboModuleRegistry.get<Spec>("JazzRelay");
