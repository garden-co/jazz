/**
 * React Native does not receive a second JavaScript/WASM Jazz runtime. This
 * package exposes the thin native-relay command transport and the source-tested
 * private JSI foreground-runtime installer; `jazz-tools` will consume that
 * installer only after a matching Rust foreground engine is embedded.
 */
// Deliberately list the public relay ABI rather than forwarding every future
// relay export through this package entry point.
export {
  NATIVE_RELAY_ABI,
  NATIVE_RELAY_ABI_V1,
  decodeNativeForegroundResponse,
  encodeNativeForegroundCommand,
  executeNativeRelayCommand,
  installNativeForegroundRuntime,
} from './relay';
export type {
  NativeForegroundCommand,
  NativeForegroundResponse,
  NativeForegroundRuntime,
  NativeForegroundRuntimeFactory,
  NativeRelayAbiRange,
} from './relay';
