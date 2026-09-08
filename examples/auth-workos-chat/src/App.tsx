import * as React from "react";
import { type User, AuthKitProvider, useAuth } from "@workos-inc/authkit-react";
import { JazzClientProvider, useAuthState } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID, WORKOS_CLIENT_ID } from "../constants.js";
import { ChatPanel } from "./ChatPanel.js";
import { AuthCard } from "./AuthCard.js";
import { createAccountManager, type AccountHandle } from "jazz-tools";
import { createJazzClient } from "jazz-tools/client";

type ChatShellProps = {
  user: User | null;
  onSignIn: () => void | Promise<void>;
  onSignOut: () => void | Promise<void>;
};

function ChatShell({ user, onSignIn, onSignOut }: ChatShellProps) {
  const { claims } = useAuthState();
  const canPostAnnouncements = claims.role === "admin";
  const canPostGeneric = claims.role === "member" || claims.role === "admin";
  const displayName = user ? `${user.firstName} ${user.lastName}`.trim() : "Anonymous";
  const statusDetail = user ? "Signed in with WorkOS" : "Sign in with WorkOS to unlock chat-01";

  return (
    <main className="app-shell">
      <section className="content-grid">
        <AuthCard
          role={claims.role as string}
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

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;

function JazzApp() {
  const { isLoading, user, getAccessToken, signIn, signOut } = useAuth();
  const [account, setAccount] = React.useState<AccountHandle>();
  const [error, setError] = React.useState<string>();
  const [accounts, setAccounts] =
    React.useState<Awaited<ReturnType<typeof createAccountManager>>>();
  const [client, setClient] = React.useState<Awaited<ReturnType<typeof createJazzClient>>>();
  const [generation, setGeneration] = React.useState(0);
  const release = React.useRef<Promise<unknown>>(Promise.resolve());
  React.useEffect(() => {
    let cancelled = false;
    setClient(undefined);
    const previousRelease = release.current;
    const creation = previousRelease.then(async () => {
      if (cancelled || !account) return;
      const next = await createJazzClient({ appId, serverUrl, account });
      if (cancelled) {
        await next.shutdown();
        return;
      }
      setClient(next);
      return next;
    });
    void creation.catch((cause: unknown) => {
      if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
    });
    return () => {
      cancelled = true;
      // A failed open owns no client and must not poison a later retry.
      // A failed prior shutdown remains a barrier against overlapping owners.
      release.current = previousRelease.then(() =>
        creation.then(
          (owned) => owned?.shutdown(),
          () => undefined,
        ),
      );
      void release.current.catch(console.error);
    };
  }, [account, generation]);
  async function leave(action: () => void | Promise<void>) {
    let closed = false;
    try {
      await client?.shutdown({ waitForSync: true });
      closed = true;
      await action();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
      if (closed) setGeneration((value) => value + 1);
    }
  }
  const userId = user?.id;
  const getToken = React.useRef(getAccessToken);
  getToken.current = getAccessToken;
  React.useEffect(() => {
    if (isLoading) return;
    let cancelled = false;
    setAccount(undefined);
    setError(undefined);
    void (async () => {
      const manager = await createAccountManager({ appId, serverUrl });
      if (cancelled) return;
      setAccounts(manager);
      const selected = userId
        ? await manager.loginJWT({ getToken: () => getToken.current({ forceRefresh: true }) })
        : (manager.getLoggedIn() ?? manager.createLocalFirst());
      if (!cancelled) setAccount(selected);
    })().catch((cause: unknown) => {
      if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
    });
    return () => {
      cancelled = true;
    };
  }, [isLoading, userId]);

  async function register() {
    if (!accounts) return;
    try {
      const selected = await accounts.registerJWT({
        getToken: () => getToken.current({ forceRefresh: true }),
      });
      setAccount(selected);
      setError(undefined);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }
  if (error && !client)
    return (
      <main>
        <p role="alert">{error}</p>
        <p>New provider identities must explicitly register with Jazz.</p>
        <button
          onClick={() => {
            void register();
          }}
        >
          Create a Jazz account
        </button>
      </main>
    );
  if (!client) return <p className="loading-state">Resolving account…</p>;
  return (
    <>
      {error && <p role="alert">{error}</p>}
      <JazzClientProvider client={client}>
        <ChatShell
          user={user}
          onSignIn={() => leave(signIn)}
          onSignOut={() => leave(() => signOut({ returnTo: window.location.href }))}
        />
      </JazzClientProvider>
    </>
  );
}

export function App() {
  return (
    <AuthKitProvider clientId={WORKOS_CLIENT_ID} devMode={true}>
      <JazzApp />
    </AuthKitProvider>
  );
}
