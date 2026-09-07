"use client";

import * as React from "react";
import { JazzSessionProvider, useJazzSession, useAuthState } from "jazz-tools/react";
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

// Track provider restoration per session, including StrictMode's replacement session.
const restored = new WeakSet<() => Promise<void>>();

export default function Page() {
  return (
    <JazzSessionProvider
      config={{ ...config, initial: "local-first" }}
      fallback={<SessionScreen />}
    >
      <SessionScreen />
    </JazzSessionProvider>
  );
}

function SessionScreen() {
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
  const [providerError, setProviderError] = React.useState<Error>();
  const error = sessionError ?? providerError;
  React.useEffect(() => {
    if (status !== "ready" || restored.has(logout)) return;
    restored.add(logout);
    void getJwtFromBetterAuth()
      .then((token) => (token ? loginJWT({ getToken }) : undefined))
      .catch((cause) =>
        setProviderError(cause instanceof Error ? cause : new Error(String(cause))),
      );
  }, [status, loginJWT, logout]);

  async function signIn(email: string, password: string) {
    const result = await authClient.signIn.email({ email, password });
    if (result.error) throw new Error(result.error.message);
    await loginJWT({ getToken });
  }
  async function signUp(email: string, password: string) {
    const result = await authClient.signUp.email({ email, name: email, password });
    if (result.error) throw new Error(result.error.message);
    await linkJWT({ getToken });
  }
  async function signOut() {
    await logout();
    await authClient.signOut();
    // This demo deliberately returns to a fresh local-first account.
    await createLocalFirst();
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
      ) : (
        <p>Preparing account…</p>
      )}
    </>
  );
}
