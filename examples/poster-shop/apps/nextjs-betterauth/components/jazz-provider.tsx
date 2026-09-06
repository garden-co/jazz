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
  const clientRef = useRef<JazzClient | undefined>(undefined);
  useEffect(() => {
    if (!session?.user) return;
    let cancelled = false;
    void (async () => {
      const accounts = await createAccountManager({ appId, serverUrl, env: "dev" });
      const token = await requireBetterAuthToken();
      const selected = accounts.getLoggedIn();
      const account: AccountHandle =
        selected?.identity.subject === session.user.id
          ? await accounts.loginJWT({ getToken: async () => token })
          : await accounts.registerJWT({ getToken: async () => token });
      const opened = await createJazzClient({ appId, serverUrl, account });
      if (cancelled) return void opened.shutdown();
      await clientRef.current?.shutdown({ waitForSync: true });
      clientRef.current = opened;
      setClient(opened);
    })();
    return () => {
      cancelled = true;
      const opened = clientRef.current;
      clientRef.current = undefined;
      setClient(undefined);
      void opened?.shutdown();
    };
  }, [session?.session.id, session?.user.id]);
  if (!session?.user || !client) return <p>Opening poster studio…</p>;
  return <JazzClientProvider client={client}>{children}</JazzClientProvider>;
}

async function requireBetterAuthToken(): Promise<string> {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Better Auth did not provide a Jazz session token.");
  return token;
}
