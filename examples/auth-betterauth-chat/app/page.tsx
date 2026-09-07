"use client";

import * as React from "react";
import {
  JazzSessionProvider,
  useJazzSessionOwner,
  useJazzSession,
  useAuthState,
} from "jazz-tools/react";
import { ChatPanel } from "../src/ChatPanel";
import { AuthCard } from "../src/AuthCard";
import { authClient, getJwtFromBetterAuth } from "../src/lib/auth-client";

type Credentials = (email: string, password: string) => Promise<void>;

function ChatShell({
  onSignIn,
  onSignUp,
  onSignOut,
}: {
  onSignIn: Credentials;
  onSignUp: Credentials;
  onSignOut: () => Promise<void>;
}): React.JSX.Element {
  const { claims, authMode, user } = useAuthState();
  const userId = user?.account ?? null;
  const role = typeof claims.role === "string" ? claims.role : null;
  const canPostAnnouncements = authMode === "external" && role === "admin";
  return (
    <main className="app-shell">
      <span data-testid="user-id" style={{ display: "none" }}>
        {userId ?? ""}
      </span>
      <section className="content-grid">
        <AuthCard
          loggedIn={authMode !== "local-first"}
          role={claims.role as string | null | undefined}
          onSignIn={onSignIn}
          onSignUp={onSignUp}
          onSignOut={onSignOut}
        />

        <ChatPanel
          chatId={process.env.NEXT_PUBLIC_ANNOUNCEMENTS_CHAT_ID!}
          title="Announcements"
          canSend={canPostAnnouncements}
          // Caveat: this demo shows the stable Jazz user id until profile fields are wired from Better Auth.
          authorName={userId}
          readOnlyNotice="Only admins can post announcements."
        />

        <ChatPanel
          chatId={process.env.NEXT_PUBLIC_CHAT_ID!}
          title={process.env.NEXT_PUBLIC_CHAT_ID!}
          canSend
          authorName={userId}
          readOnlyNotice="Sign in as admin or member to participate."
        />
      </section>
    </main>
  );
}

const config = {
  appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
  serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
};

async function getToken(): Promise<string> {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Better Auth did not provide a token");
  return token;
}

export default function Page() {
  const { session, error, retry } = useJazzSessionOwner({ ...config, initial: "local-first" });
  const [providerError, setProviderError] = React.useState<Error>();
  React.useEffect(() => {
    if (!session) return;
    void (async () => {
      const token = await getJwtFromBetterAuth();
      if (token) await session.loginJWT({ getToken });
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
  reportError: React.Dispatch<React.SetStateAction<Error | undefined>>;
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
  const clearProviderError = () =>
    reportError((current) => (current === providerError ? undefined : current));

  async function signIn(email: string, password: string) {
    const result = await authClient.signIn.email({ email, password });
    if (result.error) throw new Error(result.error.message);
    await loginJWT({ getToken });
    clearProviderError();
  }
  async function signUp(email: string, password: string) {
    const result = await authClient.signUp.email({ email, name: email, password });
    if (result.error) throw new Error(result.error.message);
    await linkJWT({ getToken });
    clearProviderError();
  }
  async function signOut() {
    reportError(undefined);
    try {
      await logout();
      const result = await authClient.signOut();
      if (result.error) throw new Error(result.error.message ?? "Provider sign-out failed");
      await createLocalFirst();
    } catch (cause) {
      reportError(cause instanceof Error ? cause : new Error(String(cause)));
      throw cause;
    }
  }
  async function registerProvider() {
    if (account?.identity.issuer === "urn:jazz:local-first") await linkJWT({ getToken });
    else await registerJWT({ getToken });
  }

  return (
    <>
      {error && (
        <div role="alert">
          <p>{error.message}</p>
          <button
            onClick={() =>
              void registerProvider()
                .then(clearProviderError)
                .catch(() => {})
            }
          >
            {account?.identity.issuer === "urn:jazz:local-first"
              ? "Link provider identity to this account"
              : "Create a new Jazz account for this provider identity"}
          </button>
          <button
            onClick={() =>
              void retry()
                .then(clearProviderError)
                .catch(() => {})
            }
          >
            Retry
          </button>
        </div>
      )}
      {status === "ready" ? (
        <ChatShell onSignIn={signIn} onSignUp={signUp} onSignOut={signOut} />
      ) : status === "signed-out" ? (
        <div>
          <button
            onClick={() =>
              void loginJWT({ getToken: getToken })
                .then(clearProviderError)
                .catch(() => {})
            }
          >
            Retry sign in
          </button>
          <button
            onClick={() =>
              void createLocalFirst()
                .then(clearProviderError)
                .catch(() => {})
            }
          >
            Continue locally
          </button>
          <button
            onClick={() =>
              void signOut()
                .then(clearProviderError)
                .catch(() => {})
            }
          >
            Retry sign out
          </button>
        </div>
      ) : (
        <p>Preparing account…</p>
      )}
    </>
  );
}
