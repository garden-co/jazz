import * as Crypto from "expo-crypto";
import * as SecureStore from "expo-secure-store";

const DEFAULT_KEY = "jazz-auth-secret";

function bytesToBase64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function generateAuthSecret(): Promise<string> {
  return bytesToBase64Url(await Crypto.getRandomBytesAsync(32));
}

export const ExpoAuthSecretStore = {
  async loadSecret(key = DEFAULT_KEY): Promise<string | null> {
    return SecureStore.getItemAsync(key);
  },

  async saveSecret(secret: string, key = DEFAULT_KEY): Promise<void> {
    await SecureStore.setItemAsync(key, secret);
  },

  async clearSecret(key = DEFAULT_KEY): Promise<void> {
    await SecureStore.deleteItemAsync(key);
  },

  async getOrCreateSecret(key = DEFAULT_KEY): Promise<string> {
    const existing = await SecureStore.getItemAsync(key);
    if (existing) return existing;

    const secret = await generateAuthSecret();
    await SecureStore.setItemAsync(key, secret);
    return secret;
  },
};
