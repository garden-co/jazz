import { NitroModules } from "react-native-nitro-modules";
import type { CoJSONCoreRN } from "./cojson-core-rn.nitro";

export const HybridCoJSONCoreRN =
  NitroModules.createHybridObject<CoJSONCoreRN>("CoJSONCoreRN");

// Export types for external use
export type {
  SessionLogHandle,
  TransactionResult,
  MakeTransactionResult,
} from "./cojson-core-rn.nitro";
