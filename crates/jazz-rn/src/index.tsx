/**
 * React Native does not receive a second Jazz runtime. This package exposes
 * only the thin native-relay command transport; `jazz-tools` will gain its
 * runtime adapter after a matching Rust relay artifact is embedded.
 */
// Deliberately list the public relay ABI rather than forwarding every future
// relay export through this package entry point.
export { NATIVE_RELAY_ABI, executeNativeRelayCommand } from './relay';
export type { NativeRelayAbiRange } from './relay';
