// Ambient declarations for React-Native / Expo internals used by the polyfills
// module. These APIs exist at runtime but don't ship public type definitions.

declare module "react-native/Libraries/Utilities/PolyfillFunctions" {
  export function polyfillGlobal(name: string, getValue: () => unknown): void;
}

declare module "react-native/Libraries/Network/fetch" {
  export const Headers: typeof globalThis.Headers | undefined;
  export const Request: typeof globalThis.Request | undefined;
  export const Response: typeof globalThis.Response | undefined;
}

declare module "expo/fetch" {
  export const fetch: typeof globalThis.fetch;
}

declare module "web-streams-polyfill" {
  export const ReadableStream: typeof globalThis.ReadableStream;
}

declare module "expo-secure-store" {
  export function getItemAsync(key: string): Promise<string | null>;
  export function setItemAsync(key: string, value: string): Promise<void>;
  export function deleteItemAsync(key: string): Promise<void>;
}
