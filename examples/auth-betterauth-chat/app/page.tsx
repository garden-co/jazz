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
      if (token) await session.loginOrRegisterJWT({ getToken });
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
  // This is the manual hybrid escape hatch: no automatic enrollment connector
  // runs while signup links the provider identity to this local-first account.
  const recovery = React.useRef<() => Promise<void>>(() =>
    session.loginOrRegisterJWT({ getToken: getToken }),
  );
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
    const result = await authClient.signIn.email({ email, password });
    if (result.error) throw new Error(result.error.message);
    await perform(() => session.loginOrRegisterJWT({ getToken: getToken }));
  }
  async function signUp(email: string, password: string) {
    const result = await authClient.signUp.email({ email, name: email, password });
    if (result.error) throw new Error(result.error.message);
    await perform(() => session.linkJWT({ getToken: getToken }));
  }
  async function signOut() {
    await perform(async () => {
      await session.logout();
      const result = await authClient.signOut();
      if (result.error) throw new Error(result.error.message ?? "Provider sign-out failed");
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
              void perform(session.status === "error" ? session.retry : recovery.current).catch(
                () => {},
              )
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
