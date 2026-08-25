/**
 * React Native does not receive a second Jazz runtime. This package exposes
 * only the thin native-relay command transport; `jazz-tools` will gain its
 * runtime adapter after a matching Rust relay artifact is embedded.
 */
export * from './relay';
