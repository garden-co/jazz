import * as React from "react";
import { type User, AuthKitProvider, useAuth } from "@workos-inc/authkit-react";
import { type DbConfig, BrowserAuthSecretStore } from "jazz-tools";
import { JazzProvider, useDb } from "jazz-tools/react";
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
  const db = useDb();
  const authState = db.getAuthState();
  const session = authState.session;
  const role = typeof session?.claims?.role === "string" ? session.claims.role : null;
  const jazzAuthenticated =
    authState.status === "authenticated" && session?.claims.auth_mode !== "local";
  const canPostAnnouncements = authState.status === "authenticated" && role === "admin";
  const canPostGeneric =
    authState.status === "authenticated" && (role === "admin" || role === "member");
  const displayName = user ? `${user.firstName} ${user.lastName}`.trim() : "Anonymous";
  const statusDetail = jazzAuthenticated
    ? "Signed in with WorkOS"
    : "Sign in with WorkOS to unlock chat-01";

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
          canSend={canPostAnnouncements}
          authorName={displayName}
          readOnlyNotice="Only admins can post announcements."
        />

        <ChatPanel
          chatId={CHAT_ID}
          title={CHAT_ID}
          canSend={canPostGeneric}
          authorName={displayName}
          readOnlyNotice="Sign in as admin or member to participate."
        />
      </section>
    </main>
  );
}

type WorkOsJazzSyncProps = React.PropsWithChildren<{
  canRefreshJwt: boolean;
  getAccessToken: () => Promise<string | null | undefined>;
}>;

function WorkOsJazzSync({ canRefreshJwt, children, getAccessToken }: WorkOsJazzSyncProps) {
  const db = useDb();

  React.useEffect(() => {
    if (!canRefreshJwt) {
      return;
    }

    return db.onAuthChanged((state) => {
      if (state.status !== "unauthenticated") {
        return;
      }

      void getAccessToken().then((accessToken) => {
        if (accessToken) {
          db.updateAuthToken(accessToken);
        }
      });
    });
  }, [canRefreshJwt, db, getAccessToken]);

  return children;
}

function JazzApp() {
  const { isLoading, user, getAccessToken, signIn, signOut } = useAuth();
  const [initialJwtToken, setInitialJwtToken] = React.useState<string | null>(null);
  const [tokenPending, setTokenPending] = React.useState(false);
  const localFirstSecret = React.use(BrowserAuthSecretStore.getOrCreateSecret());

  React.useEffect(() => {
    let cancelled = false;

    if (isLoading) {
      return;
    }

    if (!user) {
      setInitialJwtToken(null);
      setTokenPending(false);
      return;
    }

    setTokenPending(true);

    void getAccessToken().then((accessToken) => {
      if (cancelled) {
        return;
      }

      setInitialJwtToken(accessToken ?? null);
      setTokenPending(false);
    });

    return () => {
      cancelled = true;
    };
  }, [getAccessToken, isLoading, user]);

  const config = React.useMemo((): DbConfig => {
    const sharedConfig = {
      appId: DEFAULT_APP_ID,
      env: "dev" as const,
      userBranch: "main" as const,
      serverUrl: SYNC_SERVER_URL,
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
      auth: { localFirstSecret },
    };
  }, [initialJwtToken, localFirstSecret]);

  const providerKey = initialJwtToken ? "external" : `local:${localFirstSecret}`;

  if (isLoading || tokenPending) {
    return <p className="loading-state">Connecting to WorkOS...</p>;
  }

  if (user && !initialJwtToken) {
    return <p className="loading-state">Fetching WorkOS token...</p>;
  }

  return (
    <JazzProvider
      key={providerKey}
      config={config}
      fallback={<p className="loading-state">Connecting to Jazz...</p>}
    >
      <WorkOsJazzSync canRefreshJwt={Boolean(user)} getAccessToken={getAccessToken}>
        <ChatShell
          user={user}
          onSignIn={signIn}
          onSignOut={() => {
            void signOut({
              returnTo: window.location.href,
            });
          }}
        />
      </WorkOsJazzSync>
    </JazzProvider>
  );
}

export function App() {
  return (
    <AuthKitProvider clientId={WORKOS_CLIENT_ID} devMode={true}>
      <JazzApp />
    </AuthKitProvider>
  );
}
