import { NitroModules } from "react-native-nitro-modules";
import type { JazzCrypto } from "./JazzCrypto.nitro";

export const HybridJazzCrypto =
  NitroModules.createHybridObject<JazzCrypto>("JazzCrypto");
