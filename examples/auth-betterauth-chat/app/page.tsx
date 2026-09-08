"use client";

import * as React from "react";
import { JazzProvider, useJazzAuth, useAuthState } from "jazz-tools/react";
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
  const [providerError, setProviderError] = React.useState<Error>();
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
    void perform(async () => {
      const token = await getJwtFromBetterAuth();
      if (token) await actions.loginOrRegisterJWT({ getToken });
    }).catch(() => {});
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
    const result = await authClient.signIn.email({ email, password });
    if (result.error) throw new Error(result.error.message);
    await perform(() => actions.loginOrRegisterJWT({ getToken: getToken }));
  }
  async function signUp(email: string, password: string) {
    const result = await authClient.signUp.email({ email, name: email, password });
    if (result.error) throw new Error(result.error.message);
    await perform(() => actions.linkJWT({ getToken: getToken }));
  }
  async function signOut() {
    await perform(async () => {
      await actions.logout();
      const result = await authClient.signOut();
      if (result.error) throw new Error(result.error.message ?? "Provider sign-out failed");
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
                      (() => actions.loginOrRegisterJWT({ getToken: getToken }))),
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
