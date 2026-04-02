import * as React from "react";
import { type DbConfig } from "jazz-tools";
import { JazzProvider, getActiveSyntheticAuth, useDb, useSession } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID, DEFAULT_APP_ID, SYNC_SERVER_URL } from "../constants.js";
import {
  clearStoredAuthSession,
  readStoredAuthSession,
  writeStoredAuthSession,
} from "./auth-storage.js";
import { ChatPanel } from "./ChatPanel.js";
import { AuthCard } from "./AuthCard.js";
import { requestSignIn, requestSignUp } from "./api.js";

function ChatShell() {
  const db = useDb();
  const session = db.getAuthState().session;

  console.log("session", session);

  const role = typeof session?.claims?.role === "string" ? session.claims.role : null;

  async function handleSignIn(email: string, password: string) {
    const session = await requestSignIn(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, session);

    db.updateAuth(session.token);
  }

  async function handleSignUp(email: string, password: string) {
    const session = await requestSignUp(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, session);
    db.updateAuth(session.token);
  }

  function handleSignOut() {
    clearStoredAuthSession(DEFAULT_APP_ID);
    db.updateAuth(null);
  }

  React.useEffect(() => {
    return db.onAuthChanged((state) => {
      if (state.status === "unauthenticated") {
        clearStoredAuthSession(DEFAULT_APP_ID);
        db.updateAuth(null);
      }
    });
  }, [db]);

  return (
    <main className="app-shell">
      <section className="content-grid">
        <AuthCard
          loggedIn={session !== null && session?.claims.auth_mode !== "local"}
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
  const config = React.useMemo((): DbConfig => {
    const token = readStoredAuthSession(DEFAULT_APP_ID)?.token;

    return {
      appId: DEFAULT_APP_ID,
      env: "dev",
      userBranch: "main",
      serverUrl: SYNC_SERVER_URL,
      jwtToken: token,
      driver: { type: "memory" },
    };

    const localAuth = getActiveSyntheticAuth(DEFAULT_APP_ID, { defaultMode: "anonymous" });
    return {
      appId: DEFAULT_APP_ID,
      env: "dev",
      userBranch: "main",
      serverUrl: SYNC_SERVER_URL,
      localAuthMode: localAuth.localAuthMode,
      localAuthToken: localAuth.localAuthToken,
      driver: { type: "memory" },
    };
  }, []);

  return (
    <JazzProvider config={config} fallback={<p className="loading-state">Connecting to Jazz...</p>}>
      <ChatShell />
    </JazzProvider>
  );
}
