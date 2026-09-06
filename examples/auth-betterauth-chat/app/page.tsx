"use client";

import * as React from "react";
import { createAccountManager } from "jazz-tools";
import { createJazzClient } from "jazz-tools/client";
import { JazzClientProvider, useAuthState } from "jazz-tools/react";
import { ChatPanel } from "../src/ChatPanel";
import { AuthCard } from "../src/AuthCard";
import { authClient, getJwtFromBetterAuth } from "../src/lib/auth-client";

type Accounts = Awaited<ReturnType<typeof createAccountManager>>;
type Client = Awaited<ReturnType<typeof createJazzClient>>;
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

export default function Page(): React.JSX.Element {
  const [client, setClient] = React.useState<Client>();
  const [error, setError] = React.useState<string>();
  const accounts = React.useRef<Accounts | undefined>(undefined);
  const active = React.useRef<Client | undefined>(undefined);
  const transitioning = React.useRef(false);
  const mounted = React.useRef(false);

  React.useEffect(() => {
    let cancelled = false;
    mounted.current = true;
    let owned: Client | undefined;
    void (async () => {
      const manager = await createAccountManager(config);
      if (cancelled) return;
      accounts.current = manager;
      const token = await getJwtFromBetterAuth();
      if (cancelled) return;
      // Restoring provider login never implicitly registers or links an identity.
      let account = manager.getLoggedIn();
      if (token) {
        try {
          account = await manager.loginJWT({ getToken });
        } catch (cause) {
          // A failed link can leave a valid provider session and a retained
          // local account. Keep that account available and offer link retry.
          if (!account) throw cause;
          if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
        }
      } else account ??= manager.createLocalFirst();
      if (cancelled) return;
      owned = await createJazzClient({ ...config, account });
      if (cancelled) {
        await owned.shutdown();
        return;
      }
      active.current = owned;
      setClient(owned);
    })().catch((cause: unknown) => {
      if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
    });
    return () => {
      cancelled = true;
      mounted.current = false;
      const current = active.current ?? owned;
      active.current = undefined;
      void current?.shutdown();
    };
  }, []);

  async function transition(action: (manager: Accounts) => Promise<void>) {
    if (transitioning.current) throw new Error("An account operation is already running");
    const manager = accounts.current;
    if (!manager) throw new Error("Account manager is not ready");
    transitioning.current = true;
    setError(undefined);
    try {
      // Linking runs outside contexts. A failed sync barrier leaves the old
      // context usable and prevents the account operation from starting.
      await active.current?.shutdown({ waitForSync: true });
      active.current = undefined;
      setClient(undefined);
      try {
        await action(manager);
      } finally {
        const account = manager.getLoggedIn();
        if (account && mounted.current) {
          const next = await createJazzClient({ ...config, account });
          if (!mounted.current) {
            await next.shutdown();
          } else {
            active.current = next;
            setClient(next);
          }
        }
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
      throw cause;
    } finally {
      transitioning.current = false;
    }
  }

  async function signIn(email: string, password: string) {
    const result = await authClient.signIn.email({ email, password });
    if (result.error) throw new Error(result.error.message);
    await transition(async (manager) => {
      await manager.loginJWT({ getToken });
    });
  }

  async function signUp(email: string, password: string) {
    const result = await authClient.signUp.email({ email, name: email, password });
    if (result.error) throw new Error(result.error.message);
    // A fresh provider identity joins the existing Jazz account. Better Auth
    // keeps its own subject; account-owned data retains the same owner.
    await transition(async (manager) => {
      await manager.linkJWT({ getToken });
    });
  }

  async function signOut() {
    await transition(async (manager) => {
      await authClient.signOut();
      manager.logout();
      manager.createLocalFirst();
    });
  }

  async function registerProvider() {
    await transition(async (manager) => {
      if (manager.getLoggedIn()?.identity.issuer === "urn:jazz:local-first") {
        await manager.linkJWT({ getToken });
      } else await manager.registerJWT({ getToken });
    });
  }

  return (
    <>
      {error && (
        <div role="alert">
          <p>{error}</p>
          <p>
            You can link a fresh provider identity to your retained local account. Linking does not
            merge accounts.
          </p>
          <button
            onClick={() => {
              void registerProvider().catch(() => undefined);
            }}
          >
            {accounts.current?.getLoggedIn()?.identity.issuer === "urn:jazz:local-first"
              ? "Link provider identity to this account"
              : "Register provider identity as a new Jazz account"}
          </button>
        </div>
      )}
      {client ? (
        <JazzClientProvider client={client}>
          <ChatShell onSignIn={signIn} onSignUp={signUp} onSignOut={signOut} />
        </JazzClientProvider>
      ) : (
        <p className="loading-state">Preparing account…</p>
      )}
    </>
  );
}
