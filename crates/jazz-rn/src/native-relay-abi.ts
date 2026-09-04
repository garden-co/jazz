/**
 * Generated from `crates/jazz-native-relay/src/lib.rs` by
 * `scripts/generate-native-relay-abi.mjs`. Do not edit this value by hand.
 *
 * Rust owns the protocol ABI. Native hosts ask their linked artifact for this
 * value at runtime through `jazz_native_relay_abi_version()`, while TypeScript
 * imports this checked-in generated mirror before it sends any bytes.
 */
export const NATIVE_RELAY_ABI_V1 = 1 as const;

export const NATIVE_RELAY_ABI = {
  minimum: NATIVE_RELAY_ABI_V1,
  maximum: NATIVE_RELAY_ABI_V1,
} as const;
