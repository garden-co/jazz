"use client";

import { Banner, Center, Spinner } from "@astryxdesign/core";
import { useEffect, useRef, useState } from "react";
import { createAccountManager } from "jazz-tools";
import { createJazzClient, JazzClientProvider, type JazzClient } from "jazz-tools/react";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? "poster-shop-local";
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "http://127.0.0.1:4200";

export function JazzProvider({ children }: { children: React.ReactNode }) {
  const { data: session } = authClient.useSession();
  const [connection, setConnection] = useState<
    { client: JazzClient; sessionId: string; userId: string } | undefined
  >();
  const [error, setError] = useState<{ cause: Error; sessionId: string; userId: string }>();
  const clientRef = useRef<JazzClient | undefined>(undefined);
  useEffect(() => {
    if (!session?.user) return;
    let cancelled = false;
    setError(undefined);
    void (async () => {
      const accounts = await createAccountManager({ appId, serverUrl, env: "dev" });
      const credential = { getToken: requireBetterAuthToken };
      // One ordered core decision: the active assignment, or a new account
      // for a fresh identity (docs/auth/authentication).
      const account = await accounts.loginOrRegisterJWT(credential);
      const opened = await createJazzClient({ appId, serverUrl, account });
      if (cancelled) return void opened.shutdown();
      await clientRef.current?.shutdown({ waitForSync: true });
      if (cancelled) return void opened.shutdown();
      clientRef.current = opened;
      setError(undefined);
      setConnection({ client: opened, sessionId: session.session.id, userId: session.user.id });
    })().catch((cause) => {
      if (!cancelled)
        setError({
          cause: cause instanceof Error ? cause : new Error(String(cause)),
          sessionId: session.session.id,
          userId: session.user.id,
        });
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
  const visibleError =
    error?.sessionId === session.session.id && error.userId === session.user.id
      ? error.cause
      : undefined;
  if (visibleError)
    return (
      <main>
        <Center minHeight="100dvh" padding={4}>
          <Banner
            status="error"
            title="Could not open the poster studio"
            description={visibleError.message}
          />
        </Center>
      </main>
    );
  const client =
    connection?.sessionId === session.session.id && connection.userId === session.user.id
      ? connection.client
      : undefined;
  if (!client)
    return (
      <main>
        <Center minHeight="100dvh" padding={4}>
          <Spinner label="Opening the poster studio" />
        </Center>
      </main>
    );
  return <JazzClientProvider client={client}>{children}</JazzClientProvider>;
}

async function requireBetterAuthToken(): Promise<string> {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Better Auth did not provide a Jazz session token.");
  return token;
}
