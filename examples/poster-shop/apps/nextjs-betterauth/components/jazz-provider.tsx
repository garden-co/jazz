"use client";

import { useEffect, useRef, useState } from "react";
import { createAccountManager, type AccountHandle } from "jazz-tools";
import { createJazzClient, JazzClientProvider, type JazzClient } from "jazz-tools/react";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? "poster-shop-local";
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "http://127.0.0.1:4200";

export function JazzProvider({ children }: { children: React.ReactNode }) {
  const { data: session } = authClient.useSession();
  const [client, setClient] = useState<JazzClient>();
  const [error, setError] = useState<Error>();
  const clientRef = useRef<JazzClient | undefined>(undefined);
  useEffect(() => {
    if (!session?.user) return;
    let cancelled = false;
    void (async () => {
      const accounts = await createAccountManager({ appId, serverUrl, env: "dev" });
      const credential = { getToken: requireBetterAuthToken };
      let account: AccountHandle;
      try {
        account = await accounts.loginJWT(credential);
      } catch (cause) {
        if (
          !/not_registered|not found|404/i.test(
            cause instanceof Error ? cause.message : String(cause),
          )
        )
          throw cause;
        account = await accounts.registerJWT(credential);
      }
      const opened = await createJazzClient({ appId, serverUrl, account });
      if (cancelled) return void opened.shutdown();
      await clientRef.current?.shutdown({ waitForSync: true });
      clientRef.current = opened;
      setError(undefined);
      setClient(opened);
    })().catch((cause) => {
      if (!cancelled) setError(cause instanceof Error ? cause : new Error(String(cause)));
    });
    return () => {
      cancelled = true;
      const opened = clientRef.current;
      clientRef.current = undefined;
      setClient(undefined);
      void opened?.shutdown();
    };
  }, [session?.session.id, session?.user.id]);
  if (!session?.user) return <>{children}</>;
  if (error) return <p role="alert">Could not open poster studio: {error.message}</p>;
  if (!client) return <p>Opening poster studio…</p>;
  return <JazzClientProvider client={client}>{children}</JazzClientProvider>;
}

async function requireBetterAuthToken(): Promise<string> {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Better Auth did not provide a Jazz session token.");
  return token;
}
