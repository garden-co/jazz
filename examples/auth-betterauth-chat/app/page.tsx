"use client";

import * as React from "react";
import { type DbConfig } from "jazz-tools";
import { JazzProvider, useDb, useSession } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID, DEFAULT_APP_ID, SYNC_SERVER_URL } from "../constants";
import { ChatPanel } from "../../auth-simple-chat/src/ChatPanel";
import { AuthCard } from "../../auth-simple-chat/src/AuthCard";
import { authClient } from "../src/lib/auth-client";

function ChatShell() {
  const session = useSession();
  const role = typeof session?.claims?.role === "string" ? session.claims.role : null;

  async function handleSignIn(email: string, password: string) {
    const res = await authClient.signIn.email({
      email: email,
      password,
    });

    if (res.error) {
      throw new Error(res.error.message);
    }
  }

  async function handleSignUp(email: string, password: string) {
    const res = await authClient.signUp.email({
      email: email,
      name: email,
      password,
    });

    if (res.error) {
      throw new Error(res.error.message);
    }
  }

  async function handleSignOut() {
    await authClient.signOut();
  }

  return (
    <main className="app-shell">
      <section className="content-grid">
        <AuthCard
          loggedIn={session?.claims.auth_mode !== "local"}
          role={role}
          onSignIn={handleSignIn}
          onSignUp={handleSignUp}
          onSignOut={handleSignOut}
        />

        <ChatPanel
          chatId={ANNOUNCEMENTS_CHAT_ID}
          title="Announcements"
          canSend={role === "admin"}
          authorName={session?.user_id ?? null}
          readOnlyNotice="Only admins can post announcements."
        />

        <ChatPanel
          chatId={CHAT_ID}
          title={CHAT_ID}
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
  const { data: authSession, isPending: authPending } = authClient.useSession();
  const [tokenPending, setTokenPending] = React.useState(true);

  React.useEffect(() => {
    if (authPending) {
      return;
    }

    if (!authSession?.session) {
      db.updateAuth(null);
      setTokenPending(false);
      return;
    }

    const ac = new AbortController();
    setTokenPending(true);
    authClient.token().then((token) => {
      if (ac.signal.aborted) return;

      if (token.error) {
        throw new Error(token.error.message ?? "Unable to get JWT token.");
      }

      db.updateAuth(token.data.token);

      setTokenPending(false);
    });

    return () => ac.abort();
  }, [authPending, authSession?.session?.id]);

  React.useEffect(() => {
    return db.onAuthChanged((state) => {
      // if the sync server throws a 401
      // we need to try issuing a new token
      if (state.status === "unauthenticated") {
        authClient.token().then((token) => {
          if (token.error) {
            throw new Error(token.error.message ?? "Unable to get JWT token.");
          }

          db.updateAuth(token.data.token);
        });
      }
    });
  }, [db]);

  if (authPending || tokenPending) {
    return <p className="loading-state">Connecting to BetterAuth...</p>;
  }

  return children;
}

export default function Page() {
  const config = React.useMemo((): DbConfig => {
    return {
      appId: DEFAULT_APP_ID,
      env: "dev",
      userBranch: "main",
      serverUrl: SYNC_SERVER_URL,
      driver: { type: "memory" },
    };
  }, []);

  return (
    <JazzProvider config={config} fallback={<p className="loading-state">Connecting to Jazz...</p>}>
      <BetterAuthJazzSync>
        <ChatShell />
      </BetterAuthJazzSync>
    </JazzProvider>
  );
}
