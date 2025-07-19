import type { HybridObject } from "react-native-nitro-modules";

export interface JazzCrypto
  extends HybridObject<{ ios: "c++"; android: "c++" }> {
  no_args_return_string(): string;
  args_return_string(arg1: string): string;
  no_args_return_ab(): ArrayBuffer;
  args_return_ab(arg1: ArrayBuffer): ArrayBuffer;
}
