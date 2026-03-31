import * as React from "react";
import { type User, AuthKitProvider, useAuth } from "@workos-inc/authkit-react";
import { type DbConfig } from "jazz-tools";
import { JazzProvider, getActiveSyntheticAuth, useSession } from "jazz-tools/react";
import {
  ANNOUNCEMENTS_CHAT_ID,
  CHAT_ID,
  DEFAULT_APP_ID,
  WORKOS_CLIENT_ID,
  SYNC_SERVER_URL,
} from "../constants.js";
import { ChatPanel } from "../../auth-simple-chat/src/ChatPanel.js";
import { AuthCard } from "./AuthCard.js";

type ChatShellProps = {
  user: User | null;
  onSignIn: () => void | Promise<void>;
  onSignOut: () => void | Promise<void>;
};

function ChatShell({ user, onSignIn, onSignOut }: ChatShellProps) {
  const session = useSession();
  const role = typeof session?.claims?.role === "string" ? session.claims.role : null;
  const displayName = user ? `${user.firstName} ${user.lastName}`.trim() : "Anonymous";
  const statusDetail = role ? "Signed in with WorkOS" : "Sign in with WorkOS to unlock chat-01";

  return (
    <main className="app-shell">
      <section className="content-grid">
        <AuthCard
          role={role}
          statusDetail={statusDetail}
          user={user}
          onSignIn={onSignIn}
          onSignOut={onSignOut}
        />

        <ChatPanel
          chatId={ANNOUNCEMENTS_CHAT_ID}
          title="Announcements"
          canSend={role === "admin"}
          authorName={displayName}
          readOnlyNotice="Only admins can post announcements."
        />

        <ChatPanel
          chatId={CHAT_ID}
          title={CHAT_ID}
          canSend={role === "admin" || role === "member"}
          authorName={displayName}
          readOnlyNotice="Sign in as admin or member to participate."
        />
      </section>
    </main>
  );
}

type JazzAppProps = {
  token: string | null;
  onTokenChange: React.Dispatch<React.SetStateAction<string | null>>;
};

function JazzApp({ token, onTokenChange }: JazzAppProps) {
  const { isLoading, user, getAccessToken, signIn, signOut } = useAuth();

  React.useEffect(() => {
    let isCancelled = false;

    async function syncToken() {
      if (!user) {
        onTokenChange(null);
        return;
      }

      const accessToken = await getAccessToken();
      if (!isCancelled) {
        onTokenChange(accessToken ?? null);
      }
    }

    void syncToken();

    return () => {
      isCancelled = true;
    };
  }, [getAccessToken, onTokenChange, user]);

  const config = React.useMemo((): DbConfig => {
    if (token) {
      return {
        appId: DEFAULT_APP_ID,
        env: "dev",
        userBranch: "main",
        serverUrl: SYNC_SERVER_URL,
        jwtToken: token,
        driver: { type: "memory" },
      };
    }

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
  }, [token]);

  if (isLoading) {
    return <p className="loading-state">Connecting to WorkOS...</p>;
  }

  return (
    <JazzProvider
      key={token ? "jwt" : "local"}
      config={config}
      fallback={<p className="loading-state">Connecting to Jazz...</p>}
    >
      <ChatShell
        user={user}
        onSignIn={signIn}
        onSignOut={() => {
          void signOut({
            returnTo: window.location.href,
          });
        }}
      />
    </JazzProvider>
  );
}

export function App() {
  const [token, setToken] = React.useState<string | null>(null);

  return (
    <AuthKitProvider
      clientId={WORKOS_CLIENT_ID}
      devMode={true}
      onRefresh={({ accessToken }) => {
        setToken(accessToken);
      }}
    >
      <JazzApp token={token} onTokenChange={setToken} />
    </AuthKitProvider>
  );
}
