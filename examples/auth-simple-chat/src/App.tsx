import * as React from "react";
import { JazzProvider, useJazzAuth, useAuthState } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID, DEFAULT_APP_ID, SYNC_SERVER_URL } from "../constants.js";
import {
  clearStoredAuthSession,
  readStoredAuthSession,
  writeStoredAuthSession,
} from "./auth-storage.js";
import { ChatPanel } from "./ChatPanel.js";
import { AuthCard } from "./AuthCard.js";
import { requestSignIn, requestSignUp } from "./api.js";

type Credentials = (email: string, password: string) => Promise<void>;

function ChatShell({
  onSignIn,
  onSignUp,
  onSignOut,
}: {
  onSignIn: Credentials;
  onSignUp: Credentials;
  onSignOut: () => Promise<void>;
}) {
  const { authMode, claims, user } = useAuthState();
  const userId = user?.account ?? null;
  const role = typeof claims.role === "string" ? claims.role : null;
  return (
    <main className="app-shell">
      <section className="content-grid">
        <AuthCard
          loggedIn={authMode === "external"}
          role={role}
          onSignIn={onSignIn}
          onSignUp={onSignUp}
          onSignOut={onSignOut}
        />

        <ChatPanel
          chatId={ANNOUNCEMENTS_CHAT_ID}
          title="Announcements"
          canSend={role === "admin"}
          authorName={userId ?? null}
          readOnlyNotice="Only admins can post announcements."
        />

        <ChatPanel
          chatId={CHAT_ID}
          title={CHAT_ID}
          canSend={role === "admin" || role === "member"}
          authorName={userId ?? null}
          readOnlyNotice="Sign in as admin or member to participate."
        />
      </section>
    </main>
  );
}

const config = {
  appId: DEFAULT_APP_ID,
  serverUrl: SYNC_SERVER_URL,
  driver: { type: "memory" as const },
};

export function App() {
  const [providerError, setProviderError] = React.useState<Error>();
  // Retain manual link/retry intent while the provider detaches the data view.
  const recovery = React.useRef<(() => Promise<void>) | undefined>(undefined);
  const restored = React.useRef(false);
  const screen = (
    <SessionScreen
      providerError={providerError}
      reportError={setProviderError}
      recovery={recovery}
      restored={restored}
    />
  );
  return (
    <JazzProvider
      {...config}
      initial="local-first"
      signedOut={screen}
      loading={screen}
      error={screen}
    >
      {screen}
    </JazzProvider>
  );
}

async function getStoredToken() {
  const saved = readStoredAuthSession(DEFAULT_APP_ID);
  if (!saved) throw new Error("Sign in to the provider first");
  return saved.token;
}

function SessionScreen({
  providerError,
  reportError,
  recovery,
  restored,
}: {
  restored: React.RefObject<boolean>;
  providerError?: Error;
  reportError: React.Dispatch<React.SetStateAction<Error | undefined>>;
  recovery: React.RefObject<(() => Promise<void>) | undefined>;
}) {
  const { sessionActions: actions, ...session } = useJazzAuth();
  React.useEffect(() => {
    if (restored.current || session.status !== "ready") return;
    restored.current = true;
    const saved = readStoredAuthSession(DEFAULT_APP_ID);
    if (saved)
      void perform(() => actions.loginOrRegisterJWT({ getToken: getStoredToken })).catch(() => {});
  }, [session.status]);
  // This is the manual hybrid escape hatch: no automatic enrollment connector
  // runs while signup links the provider identity to this local-first account.
  async function perform(action: () => Promise<void>) {
    recovery.current = action;
    try {
      await action();
      reportError(undefined);
    } catch (cause) {
      reportError(cause instanceof Error ? cause : new Error(String(cause)));
      throw cause;
    }
  }
  async function signIn(email: string, password: string) {
    const auth = await requestSignIn(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, auth);
    await perform(() => actions.loginOrRegisterJWT({ getToken: getStoredToken }));
  }
  async function signUp(email: string, password: string) {
    const auth = await requestSignUp(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, auth);
    await perform(() => actions.linkJWT({ getToken: getStoredToken }));
  }
  async function signOut() {
    await perform(async () => {
      await actions.logout();
      clearStoredAuthSession(DEFAULT_APP_ID);
      await actions.createLocalFirst();
    });
  }
  const error = providerError ?? session.error;
  return (
    <>
      {error && (
        <section role="alert">
          <p>{error.message}</p>
          <button
            onClick={() =>
              void perform(
                session.error
                  ? session.retry
                  : (recovery.current ??
                      (() => actions.loginOrRegisterJWT({ getToken: getStoredToken }))),
              ).catch(() => {})
            }
          >
            Retry
          </button>
        </section>
      )}
      {session.status === "ready" ? (
        <ChatShell onSignIn={signIn} onSignUp={signUp} onSignOut={signOut} />
      ) : (
        <p>Preparing account...</p>
      )}
    </>
  );
}
