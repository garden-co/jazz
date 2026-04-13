"use client";

import * as React from "react";
import { type DbConfig } from "jazz-tools";
import { JazzProvider, getActiveSyntheticAuth, useDb } from "jazz-tools/react";
import { ChatPanel } from "../../auth-simple-chat/src/ChatPanel";
import { AuthCard } from "../../auth-simple-chat/src/AuthCard";
import { authClient, getJwtFromBetterAuth } from "../src/lib/auth-client";

function ChatShell(): React.JSX.Element {
  const db = useDb();
  const authState = db.getAuthState();
  const session = authState.session;
  const role = typeof session?.claims?.role === "string" ? session.claims.role : null;
  const canPostAnnouncements = authState.status === "authenticated" && role === "admin";
  const canPostGeneric =
    authState.status === "authenticated" && (role === "admin" || role === "member");

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
    const res = await authClient.signUp.email({
      email,
      name: email,
      password,
    });

    if (res.error) {
      throw new Error(res.error.message);
    }
  }

  async function handleSignOut() {
    await authClient.signOut();
    // Changing the principal ID is not supported yet
    // so we need to reload the page to clear the auth state
    window.location.reload();
  }

  return (
    <main className="app-shell">
      <section className="content-grid">
        <AuthCard
          loggedIn={authState.status === "authenticated" && session?.claims.auth_mode !== "local"}
          role={role}
          onSignIn={handleSignIn}
          onSignUp={handleSignUp}
          onSignOut={handleSignOut}
        />

        <ChatPanel
          chatId={process.env.NEXT_PUBLIC_ANNOUNCEMENTS_CHAT_ID!}
          title="Announcements"
          canSend={canPostAnnouncements}
          authorName={session?.user_id ?? null}
          readOnlyNotice="Only admins can post announcements."
        />

        <ChatPanel
          chatId={process.env.NEXT_PUBLIC_CHAT_ID!}
          title={process.env.NEXT_PUBLIC_CHAT_ID!}
          canSend={role === "admin" || role === "member"}
          authorName={session?.user_id ?? null}
          readOnlyNotice="Sign in as admin or member to participate."
        />
      </section>
    </main>
  );
}

function BetterAuthJazzSync({ children }: React.PropsWithChildren<{}>) {
  const db = useDb();

  React.useEffect(() => {
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
  }, [db]);

  return children;
}

export default function Page(): React.JSX.Element {
  const { data: authSession, isPending: authPending } = authClient.useSession();
  const [initialJwtToken, setInitialJwtToken] = React.useState<string | null>(null);
  const [tokenPending, setTokenPending] = React.useState(true);
  const localAuth = React.useMemo(
    () => getActiveSyntheticAuth(process.env.NEXT_PUBLIC_APP_ID!, { defaultMode: "anonymous" }),
    [],
  );

  React.useEffect(() => {
    if (authPending) {
      return;
    }

    const ac = new AbortController();
    async function syncJazzAuth() {
      setTokenPending(true);

      if (!authSession?.session) {
        if (!ac.signal.aborted) {
          setTokenPending(false);
        }
        return;
      }

      const jwtToken = await getJwtFromBetterAuth();
      if (ac.signal.aborted) {
        return;
      }

      setInitialJwtToken(jwtToken);
      setTokenPending(false);
    }

    void syncJazzAuth();

    return () => ac.abort();
  }, [authPending]);

  const config = React.useMemo((): DbConfig => {
    const sharedConfig = {
      appId: process.env.NEXT_PUBLIC_APP_ID!,
      env: "dev" as const,
      userBranch: "main" as const,
      serverUrl: process.env.NEXT_PUBLIC_SYNC_SERVER_URL!,
      driver: { type: "memory" as const },
    };

    if (initialJwtToken) {
      return {
        ...sharedConfig,
        jwtToken: initialJwtToken,
      };
    }

    return {
      ...sharedConfig,
      localAuthMode: localAuth.localAuthMode,
      localAuthToken: localAuth.localAuthToken,
    };
  }, [initialJwtToken, localAuth.localAuthMode, localAuth.localAuthToken]);

  if (authPending || tokenPending) {
    return <p className="loading-state">Connecting to BetterAuth...</p>;
  }

  if (authPending && !initialJwtToken) {
    return <p className="loading-state">Fetching BetterAuth token...</p>;
  }

  return (
    <JazzProvider config={config} fallback={<p className="loading-state">Connecting to Jazz...</p>}>
      <BetterAuthJazzSync>
        <ChatShell />
      </BetterAuthJazzSync>
    </JazzProvider>
  );
}
