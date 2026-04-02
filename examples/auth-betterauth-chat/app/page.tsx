"use client";

import * as React from "react";
import { type DbConfig } from "jazz-tools";
import { JazzProvider, useDb, useSession } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID, DEFAULT_APP_ID, SYNC_SERVER_URL } from "../constants";
import { ChatPanel } from "../../auth-simple-chat/src/ChatPanel";
import { AuthCard } from "../../auth-simple-chat/src/AuthCard";
import { authClient } from "../src/lib/auth-client";

async function getJazzJwtFromBetterAuth(): Promise<string | null> {
  try {
    const token = await authClient.token();
    if (token.error) {
      console.error("Error getting JWT token:", token.error.message);
      return null;
    }

    return token.data.token;
  } catch (error) {
    console.error("Error getting JWT token:", error);
    return null;
  }
}

function ChatShell() {
  const db = useDb();
  const authState = db.getAuthState();
  const session = authState.session;
  const role = typeof session?.claims?.role === "string" ? session.claims.role : null;

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

    const ac = new AbortController();
    async function syncJazzAuth() {
      setTokenPending(true);

      if (!authSession?.session) {
        if (!ac.signal.aborted) {
          db.updateAuth(null);
          setTokenPending(false);
        }
        return;
      }

      const jwtToken = await getJazzJwtFromBetterAuth();
      if (ac.signal.aborted) {
        return;
      }

      db.updateAuth(jwtToken);
      setTokenPending(false);
    }

    void syncJazzAuth();

    return () => ac.abort();
  }, [authPending, authSession?.session?.id, db]);

  React.useEffect(() => {
    return db.onAuthChanged((state) => {
      // if the sync server throws a 401
      // we need to try issuing a new token
      if (state.status === "unauthenticated") {
        if (!authSession?.session) {
          db.updateAuth(null);
          return;
        }

        void getJazzJwtFromBetterAuth().then((jwtToken) => {
          db.updateAuth(jwtToken);
        });
      }
    });
  }, [authSession?.session?.id, db]);

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
