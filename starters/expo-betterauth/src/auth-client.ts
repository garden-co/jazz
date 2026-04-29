import { Platform } from "react-native";
import { createAuthClient } from "better-auth/react";
import { expoClient } from "@better-auth/expo/client";
import { jwtClient } from "better-auth/client/plugins";
import * as SecureStore from "expo-secure-store";

declare const process: { env: Record<string, string | undefined> };

function resolveServerUrl(): string {
  const explicit = process.env.EXPO_PUBLIC_AUTH_SERVER_URL;
  if (explicit) return explicit;
  if (Platform.OS === "android") return "http://10.0.2.2:3001";
  return "http://localhost:3001";
}

export const authBaseURL = resolveServerUrl();

export const authClient = createAuthClient({
  baseURL: authBaseURL,
  plugins: [
    jwtClient(),
    expoClient({
      scheme: "expobetterauth",
      storagePrefix: "expo-betterauth",
      storage: SecureStore,
    }),
  ],
});

export const { useSession } = authClient;
