"use client";

import { useEffect, useRef, useState } from "react";
import { createAccountManager } from "jazz-tools";
import { createJazzClient, JazzClientProvider, type JazzClient } from "jazz-tools/react";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";
import { loginOrRegister } from "@/src/lib/account-enrollment";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? "poster-shop-local";
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "http://127.0.0.1:4200";

export function JazzProvider({ children }: { children: React.ReactNode }) {
  const { data: session } = authClient.useSession();
  const [connection, setConnection] = useState<
    { client: JazzClient; sessionId: string; userId: string } | undefined
  >();
  const [error, setError] = useState<Error>();
  const clientRef = useRef<JazzClient | undefined>(undefined);
  useEffect(() => {
    if (!session?.user) return;
    let cancelled = false;
    void (async () => {
      const accounts = await createAccountManager({ appId, serverUrl, env: "dev" });
      const credential = { getToken: requireBetterAuthToken };
      const account = await loginOrRegister(accounts, credential);
      const opened = await createJazzClient({ appId, serverUrl, account });
      if (cancelled) return void opened.shutdown();
      await clientRef.current?.shutdown({ waitForSync: true });
      clientRef.current = opened;
      setError(undefined);
      setConnection({ client: opened, sessionId: session.session.id, userId: session.user.id });
    })().catch((cause) => {
      if (!cancelled) setError(cause instanceof Error ? cause : new Error(String(cause)));
    });
    return () => {
      cancelled = true;
      const opened = clientRef.current;
      clientRef.current = undefined;
      setConnection(undefined);
      void opened?.shutdown();
    };
  }, [session?.session.id, session?.user.id]);
  if (!session?.user) return <>{children}</>;
  if (error) return <p role="alert">Could not open poster studio: {error.message}</p>;
  const client =
    connection?.sessionId === session.session.id && connection.userId === session.user.id
      ? connection.client
      : undefined;
  if (!client) return <p>Opening poster studio…</p>;
  return <JazzClientProvider client={client}>{children}</JazzClientProvider>;
}

async function requireBetterAuthToken(): Promise<string> {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Better Auth did not provide a Jazz session token.");
  return token;
}
