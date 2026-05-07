// Ambient declarations for React-Native / Expo internals used by the polyfills
// module. These APIs exist at runtime but don't ship public type definitions.

declare module "react-native/Libraries/Utilities/PolyfillFunctions" {
  export function polyfillGlobal(name: string, getValue: () => unknown): void;
}

declare module "web-streams-polyfill" {
  export const ReadableStream: typeof globalThis.ReadableStream;
}

declare module "expo-crypto" {
  export function getRandomBytes(byteCount: number): Uint8Array;
}

declare module "expo-secure-store" {
  export function getItemAsync(key: string): Promise<string | null>;
  export function setItemAsync(key: string, value: string): Promise<void>;
  export function deleteItemAsync(key: string): Promise<void>;
}
