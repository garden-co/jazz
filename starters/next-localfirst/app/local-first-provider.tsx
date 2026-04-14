"use client";

import { useEffect, useState } from "react";
import { type DbConfig, BrowserAuthSecretStore } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";

export function LocalFirstProvider({ children }: React.PropsWithChildren) {
  const [config, setConfig] = useState<DbConfig | null>(null);

  useEffect(() => {
    BrowserAuthSecretStore.getOrCreateSecret().then((secret) => {
      setConfig(buildConfig(secret));
    });
  }, []);

  if (!config) return null;

  return (
    <JazzProvider config={config} fallback={<p>Loading...</p>}>
      {children}
    </JazzProvider>
  );
}

function buildConfig(localFirstSecret: string): DbConfig {
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
  if (!appId) {
    throw new Error(
      "NEXT_PUBLIC_JAZZ_APP_ID is not set. The withJazz Next plugin injects this at dev time; in production, set it explicitly in your environment.",
    );
  }
  return {
    appId,
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "ws://localhost:1625",
    env: "dev",
    userBranch: "main",
    driver: { type: "memory" },
    auth: { localFirstSecret },
  };
}
