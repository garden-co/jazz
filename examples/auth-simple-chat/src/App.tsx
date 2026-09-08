import * as React from "react";
import {
  JazzSessionProvider,
  useJazzSessionOwner,
  useJazzSession,
  useAuthState,
} from "jazz-tools/react";
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
  const { session, error, retry } = useJazzSessionOwner({ ...config, initial: "local-first" });
  const [providerError, setProviderError] = React.useState<Error>();
  // Keep retry intent above the provider: its ready and fallback trees remount.
  const recovery = React.useRef<(() => Promise<void>) | undefined>(undefined);
  React.useEffect(() => {
    if (!session) return;
    void (async () => {
      const saved = readStoredAuthSession(DEFAULT_APP_ID);
      if (saved) await session.loginOrRegisterJWT({ getToken: async () => saved.token });
    })().catch((cause) =>
      setProviderError(cause instanceof Error ? cause : new Error(String(cause))),
    );
  }, [session]);
  if (!session)
    return error ? (
      <p role="alert">
        {error.message} <button onClick={() => void retry().catch(() => {})}>Retry</button>
      </p>
    ) : (
      <p>Preparing account…</p>
    );
  const screen = (
    <SessionScreen
      providerError={providerError}
      reportError={setProviderError}
      recovery={recovery}
    />
  );
  return (
    <JazzSessionProvider session={session} fallback={screen}>
      {screen}
    </JazzSessionProvider>
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
}: {
  providerError?: Error;
  reportError: React.Dispatch<React.SetStateAction<Error | undefined>>;
  recovery: React.RefObject<(() => Promise<void>) | undefined>;
}) {
  const session = useJazzSession();
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
    await perform(() => session.loginOrRegisterJWT({ getToken: getStoredToken }));
  }
  async function signUp(email: string, password: string) {
    const auth = await requestSignUp(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, auth);
    await perform(() => session.linkJWT({ getToken: getStoredToken }));
  }
  async function signOut() {
    await perform(async () => {
      await session.logout();
      clearStoredAuthSession(DEFAULT_APP_ID);
      await session.createLocalFirst();
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
                session.status === "error"
                  ? session.retry
                  : (recovery.current ??
                      (() => session.loginOrRegisterJWT({ getToken: getStoredToken }))),
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
