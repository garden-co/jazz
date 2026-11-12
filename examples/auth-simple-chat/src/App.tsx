import * as React from "react";
import { JazzProvider, useJazzSession, useAuthState } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "../constants.js";
import { ChatPanel } from "./ChatPanel.js";
import { AuthCard } from "./AuthCard.js";
import { fetchSignedInUser, requestSignIn, requestSignOut, requestSignUp } from "./api.js";

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
  const userId = user;
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

// Jazz runs inside the auth server, reached on this origin under /jazz.
const config = {
  serverUrl: "/jazz",
  driver: { type: "memory" as const },
  initial: "device" as const,
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
    <JazzProvider config={config} signedOut={screen} loading={screen} error={screen}>
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
  const session = useJazzSession();
  React.useEffect(() => {
    if (restored.current || session.status !== "ready") return;
    restored.current = true;
    void perform(async () => {
      if (await fetchSignedInUser()) await session.refresh();
    }).catch(() => {});
  }, [session.status]);
  // This is the manual hybrid escape hatch: no automatic enrollment connector
  // runs while signup links the provider identity to this device account.
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
    await requestSignIn(email, password);
    await perform(() => session.refresh());
  }
  async function signUp(email: string, password: string) {
    await requestSignUp(email, password);
    // The backend links this browser's device account to the new user,
    // so everything written as a guest stays theirs.
    await perform(() => session.linkDeviceAccount());
  }
  async function signOut() {
    await perform(async () => {
      await session.signOut();
      await requestSignOut();
      await session.createDeviceAccount();
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
                session.error ? session.retry : (recovery.current ?? (() => session.refresh())),
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
