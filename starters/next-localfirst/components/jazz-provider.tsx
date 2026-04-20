"use client";

import { useEffect, useState } from "react";
import { type DbConfig, BrowserAuthSecretStore } from "jazz-tools";
import { JazzProvider as JazzBaseProvider } from "jazz-tools/react";

const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;

export function JazzProvider({ children }: React.PropsWithChildren) {
  const [config, setConfig] = useState<DbConfig | null>(null);

  useEffect(() => {
    BrowserAuthSecretStore.getOrCreateSecret().then((secret) => {
      setConfig(buildConfig(secret));
    });
  }, []);

  if (!config) return null;

  return (
    <JazzBaseProvider config={config} fallback={<p>Loading...</p>}>
      {children}
    </JazzBaseProvider>
  );
}

function buildConfig(secret: string): DbConfig {
  if (!APP_ID || !SERVER_URL) {
    const missing = [
      !APP_ID && "NEXT_PUBLIC_JAZZ_APP_ID",
      !SERVER_URL && "NEXT_PUBLIC_JAZZ_SERVER_URL",
    ]
      .filter((v) => !!v)
      .join(" & ");
    throw new Error(
      `${missing} not set. The withJazz Next plugin injects these at dev time; in production, set them explicitly in your environment.`,
    );
  }
  return {
    appId: APP_ID,
    serverUrl: SERVER_URL,
    secret,
  };
}
