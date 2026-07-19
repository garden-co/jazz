import * as React from "react";
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

export function useExpoLocalFirstAuth(): {
  secret: string | null;
  isLoading: boolean;
  login(secret: string): Promise<void>;
} {
  const [secret, setSecret] = React.useState<string | null>(null);
  const [isLoading, setIsLoading] = React.useState(true);

  React.useEffect(() => {
    let cancelled = false;

    ExpoAuthSecretStore.getOrCreateSecret()
      .then((nextSecret) => {
        if (!cancelled) setSecret(nextSecret);
      })
      .finally(() => {
        if (!cancelled) setIsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const login = React.useCallback(async (nextSecret: string) => {
    await ExpoAuthSecretStore.saveSecret(nextSecret);
    setSecret(nextSecret);
  }, []);

  return { secret, isLoading, login };
}
