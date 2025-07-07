import { NitroModules } from "react-native-nitro-modules";
import type { JazzCrypto } from "./JazzCrypto.nitro";

export const HybridJazzCrypto =
  NitroModules.createHybridObject<JazzCrypto>("JazzCrypto");

export function no_args_return_string() {
  return HybridJazzCrypto.no_args_return_string();
}

export function args_return_string(arg1: string) {
  return HybridJazzCrypto.args_return_string(arg1);
}

export function no_args_return_ab() {
  return HybridJazzCrypto.no_args_return_ab();
}

export function args_return_ab(arg1: ArrayBuffer) {
  return HybridJazzCrypto.args_return_ab(arg1);
}
