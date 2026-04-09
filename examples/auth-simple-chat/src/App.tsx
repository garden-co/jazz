import * as React from "react";
import { type DbConfig } from "jazz-tools";
import { JazzProvider, useDb } from "jazz-tools/react";
import { loadOrCreateIdentitySeed, mintSelfSignedToken } from "jazz-tools";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID, DEFAULT_APP_ID, SYNC_SERVER_URL } from "../constants.js";
import {
  clearStoredAuthSession,
  readStoredAuthSession,
  type StoredAuthSession,
  writeStoredAuthSession,
} from "./auth-storage.js";
import { ChatPanel } from "./ChatPanel.js";
import { AuthCard } from "./AuthCard.js";
import { requestSignIn, requestSignUp } from "./api.js";

type ChatShellProps = {
  onStoredAuthSessionChange(session: StoredAuthSession | null): void;
};

function ChatShell({ onStoredAuthSessionChange }: ChatShellProps) {
  const db = useDb();
  const authState = db.getAuthState();
  const session = authState.session;

  const role = typeof session?.claims?.role === "string" ? session.claims.role : null;

  async function handleSignIn(email: string, password: string) {
    const session = await requestSignIn(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, session);
    onStoredAuthSessionChange(session);
  }

  async function handleSignUp(email: string, password: string) {
    const session = await requestSignUp(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, session);
    onStoredAuthSessionChange(session);
  }

  function handleSignOut() {
    clearStoredAuthSession(DEFAULT_APP_ID);
    onStoredAuthSessionChange(null);
  }

  React.useEffect(() => {
    return db.onAuthChanged((state) => {
      // React to sync-server 401s
      if (state.status === "unauthenticated") {
        clearStoredAuthSession(DEFAULT_APP_ID);
        onStoredAuthSessionChange(null);
      }
    });
  }, [db, onStoredAuthSessionChange]);

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

export function App() {
  const [storedAuthSession, setStoredAuthSession] = React.useState<StoredAuthSession | null>(() =>
    readStoredAuthSession(DEFAULT_APP_ID),
  );
  const selfSignedToken = React.useMemo(() => {
    const seed = loadOrCreateIdentitySeed(DEFAULT_APP_ID);
    return mintSelfSignedToken(seed.seed, DEFAULT_APP_ID);
  }, []);

  const config = React.useMemo((): DbConfig => {
    const sharedConfig = {
      appId: DEFAULT_APP_ID,
      env: "dev" as const,
      userBranch: "main" as const,
      serverUrl: SYNC_SERVER_URL,
      driver: { type: "memory" as const },
    };

    if (storedAuthSession) {
      return {
        ...sharedConfig,
        jwtToken: storedAuthSession.token,
      };
    }

    return {
      ...sharedConfig,
      jwtToken: selfSignedToken,
    };
  }, [selfSignedToken, storedAuthSession]);

  return (
    <JazzProvider config={config} fallback={<p className="loading-state">Connecting to Jazz...</p>}>
      <ChatShell onStoredAuthSessionChange={setStoredAuthSession} />
    </JazzProvider>
  );
}
