"use client";

import * as React from "react";
import { type DbConfig, LocalStorageAuthSecretStore } from "jazz-tools";
import { JazzProvider, useDb } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "../constants";
import { ChatPanel } from "../../auth-simple-chat/src/ChatPanel";
import { AuthCard } from "../../auth-simple-chat/src/AuthCard";
import { authClient, getJwtFromBetterAuth } from "../src/lib/auth-client";

const authSecretStore = new LocalStorageAuthSecretStore();

function ChatShell(): React.JSX.Element {
  const db = useDb();
  const authState = db.getAuthState();
  const session = authState.session;
  const role = typeof session?.claims?.role === "string" ? session.claims.role : null;
  const canPostAnnouncements = authState.status === "authenticated" && role === "admin";
  const canPostGeneric = authState.status === "authenticated";

  async function handleSignIn(email: string, password: string) {
    const res = await authClient.signIn.email({
      email,
      password,
    });

    if (res.error) {
      throw new Error(res.error.message);
    }
  }

  async function handleSignUp(email: string, password: string) {
    const proofToken = await db.getSelfSignedToken({
      ttlSeconds: 60,
      audience: "betterauth-signup",
    });

    if (!proofToken) {
      throw new Error("Sign up requires an active Jazz session");
    }

    const res = await authClient.signUp.email({
      email,
      name: email,
      password,
      proofToken,
    });

    if (res.error) {
      throw new Error(res.error.message);
    }
  }

  async function handleSignOut() {
    await authClient.signOut();
    await authSecretStore.clearSecret();
  }

  const authMode = session?.claims.auth_mode;

  return (
    <main className="app-shell">
      <span data-testid="user-id" style={{ display: "none" }}>
        {session?.user_id ?? ""}
      </span>
      <section className="content-grid">
        <AuthCard
          loggedIn={authState.status === "authenticated" && authMode !== "self-signed"}
          role={role}
          onSignIn={handleSignIn}
          onSignUp={handleSignUp}
          onSignOut={handleSignOut}
        />

        <ChatPanel
          chatId={ANNOUNCEMENTS_CHAT_ID}
          title="Announcements"
          canSend={canPostAnnouncements}
          authorName={session?.user_id ?? null}
          readOnlyNotice="Only admins can post announcements."
        />

        <ChatPanel
          chatId={CHAT_ID}
          title={CHAT_ID}
          canSend={canPostGeneric}
          authorName={session?.user_id ?? null}
          readOnlyNotice="Sign in as admin or member to participate."
        />
      </section>
    </main>
  );
}

function BetterAuthJazzSync({
  hasBetterAuthSession,
  children,
}: React.PropsWithChildren<{ hasBetterAuthSession: boolean }>) {
  const db = useDb();

  React.useEffect(() => {
    if (!hasBetterAuthSession) return;

    async function refreshJazzAuthToken(): Promise<void> {
      const jwtToken = await getJwtFromBetterAuth();
      if (jwtToken) {
        db.updateAuthToken(jwtToken);
      }
    }

    return db.onAuthChanged((state) => {
      if (state.status !== "unauthenticated") {
        return;
      }

      void refreshJazzAuthToken();
    });
  }, [db, hasBetterAuthSession]);

  return children;
}

export default function Page(): React.JSX.Element {
  const { data: authSession, isPending: authPending } = authClient.useSession();
  const [config, setConfig] = React.useState<DbConfig | null>(null);

  React.useEffect(() => {
    if (authPending) return;

    const ac = new AbortController();

    async function resolveConfig() {
      const sharedConfig = {
        appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
        env: "dev" as const,
        userBranch: "main" as const,
        serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
        driver: { type: "memory" as const },
      };

      if (authSession?.session) {
        const jwtToken = await getJwtFromBetterAuth();
        if (ac.signal.aborted) return;
        setConfig({ ...sharedConfig, jwtToken: jwtToken! });
      } else {
        const secret = await authSecretStore.getOrCreateSecret();
        if (ac.signal.aborted) return;
        setConfig({ ...sharedConfig, auth: { seed: secret } });
      }
    }

    void resolveConfig();
    return () => ac.abort();
  }, [authPending, authSession?.session]);

  if (authPending || !config) {
    return <p className="loading-state">Connecting to BetterAuth...</p>;
  }

  return (
    <JazzProvider config={config} fallback={<p className="loading-state">Connecting to Jazz...</p>}>
      <BetterAuthJazzSync hasBetterAuthSession={!!authSession?.session}>
        <ChatShell />
      </BetterAuthJazzSync>
    </JazzProvider>
  );
}
