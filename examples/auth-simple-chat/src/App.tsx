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
  React.useEffect(() => {
    if (!session) return;
    void (async () => {
      const saved = readStoredAuthSession(DEFAULT_APP_ID);
      if (saved) await session.loginJWT({ getToken: async () => saved.token });
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
  const screen = <SessionScreen providerError={providerError} reportError={setProviderError} />;
  return (
    <JazzSessionProvider session={session} fallback={screen}>
      {screen}
    </JazzSessionProvider>
  );
}

function SessionScreen({
  providerError,
  reportError,
}: {
  providerError?: Error;
  reportError(error: Error | undefined): void;
}) {
  const session = useJazzSession();
  const {
    account,
    status,
    error: sessionError,
    loginJWT,
    linkJWT,
    registerJWT,
    logout,
    createLocalFirst,
    retry,
  } = session;
  const error = sessionError ?? providerError;

  async function signIn(email: string, password: string) {
    const auth = await requestSignIn(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, auth);
    await loginJWT({ getToken: async () => auth.token });
  }
  async function signUp(email: string, password: string) {
    const auth = await requestSignUp(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, auth);
    await linkJWT({ getToken: async () => auth.token });
  }
  async function signOut() {
    reportError(undefined);
    try {
      await logout();
      clearStoredAuthSession(DEFAULT_APP_ID);
      await createLocalFirst();
    } catch (cause) {
      reportError(cause instanceof Error ? cause : new Error(String(cause)));
      throw cause;
    }
  }
  async function registerProvider() {
    const auth = readStoredAuthSession(DEFAULT_APP_ID);
    if (!auth) throw new Error("Sign in to the provider first");
    const getToken = async () => auth.token;
    if (account?.identity.issuer === "urn:jazz:local-first") await linkJWT({ getToken });
    else await registerJWT({ getToken });
  }

  return (
    <>
      {error && (
        <div role="alert">
          <p>{error.message}</p>
          <button onClick={() => void registerProvider().catch(() => {})}>
            {account?.identity.issuer === "urn:jazz:local-first"
              ? "Link provider identity to this account"
              : "Create a new Jazz account for this provider identity"}
          </button>
          <button onClick={() => void retry().catch(() => {})}>Retry</button>
        </div>
      )}
      {status === "ready" ? (
        <ChatShell onSignIn={signIn} onSignUp={signUp} onSignOut={signOut} />
      ) : status === "signed-out" ? (
        <div>
          <button
            onClick={() =>
              void loginJWT({
                getToken: async () => {
                  const saved = readStoredAuthSession(DEFAULT_APP_ID);
                  if (!saved) throw new Error("Sign in to the provider first");
                  return saved.token;
                },
              }).catch(() => {})
            }
          >
            Retry sign in
          </button>
          <button onClick={() => void createLocalFirst().catch(() => {})}>Continue locally</button>
          <button onClick={() => void signOut().catch(() => {})}>Retry sign out</button>
        </div>
      ) : (
        <p>Preparing account…</p>
      )}
    </>
  );
}
