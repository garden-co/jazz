declare module "react-native/Libraries/Utilities/PolyfillFunctions" {
  export function polyfillGlobal(name: string, getValue: () => unknown): void;
}

declare module "web-streams-polyfill" {
  export const ReadableStream: typeof globalThis.ReadableStream;
}

declare module "expo-crypto" {
  export function getRandomBytes(byteCount: number): Uint8Array;
  export enum CryptoDigestAlgorithm {
    SHA256 = "SHA-256",
  }
  export function digestStringAsync(
    algorithm: CryptoDigestAlgorithm,
    data: string,
  ): Promise<string>;
}

declare module "expo-secure-store" {
  export function getItem(key: string): string | null;
  export function setItem(key: string, value: string): void;
  export function getItemAsync(key: string): Promise<string | null>;
  export function setItemAsync(key: string, value: string): Promise<void>;
  export function deleteItemAsync(key: string): Promise<void>;
}
