import * as React from "react";
import { createAccountManager } from "jazz-tools";
import { createJazzClient } from "jazz-tools/client";
import { JazzClientProvider, useAuthState } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID, DEFAULT_APP_ID, SYNC_SERVER_URL } from "../constants.js";
import {
  clearStoredAuthSession,
  readStoredAuthSession,
  writeStoredAuthSession,
} from "./auth-storage.js";
import { ChatPanel } from "./ChatPanel.js";
import { AuthCard } from "./AuthCard.js";
import { requestSignIn, requestSignUp } from "./api.js";

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
  const [client, setClient] = React.useState<Client>();
  const [error, setError] = React.useState<string>();
  const accounts = React.useRef<Accounts | undefined>(undefined);
  const active = React.useRef<Client | undefined>(undefined);
  const mounted = React.useRef(false);
  const busy = React.useRef(false);
  React.useEffect(() => {
    let cancelled = false;
    mounted.current = true;
    void (async () => {
      const manager = await createAccountManager(config);
      if (cancelled) return;
      accounts.current = manager;
      const saved = readStoredAuthSession(DEFAULT_APP_ID);
      let account = manager.getLoggedIn();
      if (saved) {
        try {
          account = await manager.loginJWT({ getToken: async () => saved.token });
        } catch (cause) {
          if (!account) throw cause;
          if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
        }
      } else account ??= manager.createLocalFirst();
      if (cancelled) return;
      const next = await createJazzClient({ ...config, account });
      if (cancelled) {
        await next.shutdown();
        return;
      }
      active.current = next;
      setClient(next);
    })().catch((cause: unknown) => {
      if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
    });
    return () => {
      cancelled = true;
      mounted.current = false;
      const owned = active.current;
      active.current = undefined;
      void owned?.shutdown();
    };
  }, []);

  async function transition(action: (manager: Accounts) => Promise<void>) {
    if (busy.current) throw new Error("An account operation is already running");
    const manager = accounts.current;
    if (!manager) throw new Error("Accounts are not ready");
    busy.current = true;
    setError(undefined);
    try {
      await active.current?.shutdown({ waitForSync: true });
      active.current = undefined;
      setClient(undefined);
      try {
        await action(manager);
      } finally {
        const account = manager.getLoggedIn();
        if (account && mounted.current) {
          const next = await createJazzClient({ ...config, account });
          if (!mounted.current) await next.shutdown();
          else {
            active.current = next;
            setClient(next);
          }
        }
      }
    } catch (cause) {
      if (mounted.current) setError(cause instanceof Error ? cause.message : String(cause));
      throw cause;
    } finally {
      busy.current = false;
    }
  }

  async function signIn(email: string, password: string) {
    const session = await requestSignIn(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, session);
    await transition(async (manager) => {
      await manager.loginJWT({ getToken: async () => session.token });
    });
  }
  async function signUp(email: string, password: string) {
    const session = await requestSignUp(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, session);
    await transition(async (manager) => {
      await manager.linkJWT({ getToken: async () => session.token });
    });
  }
  async function signOut() {
    await transition(async (manager) => {
      clearStoredAuthSession(DEFAULT_APP_ID);
      manager.logout();
      manager.createLocalFirst();
    });
  }
  async function register() {
    const session = readStoredAuthSession(DEFAULT_APP_ID);
    if (!session) throw new Error("Sign in to the provider first");
    await transition(async (manager) => {
      if (manager.getLoggedIn()?.identity.issuer === "urn:jazz:local-first") {
        await manager.linkJWT({ getToken: async () => session.token });
      } else await manager.registerJWT({ getToken: async () => session.token });
    });
  }
  return (
    <>
      {error && (
        <div role="alert">
          <p>{error}</p>
          <button
            onClick={() => {
              void register().catch(() => undefined);
            }}
          >
            {accounts.current?.getLoggedIn()?.identity.issuer === "urn:jazz:local-first"
              ? "Link provider identity to this account"
              : "Create a new Jazz account for this provider identity"}
          </button>
        </div>
      )}
      {client ? (
        <JazzClientProvider client={client}>
          <ChatShell onSignIn={signIn} onSignUp={signUp} onSignOut={signOut} />
        </JazzClientProvider>
      ) : (
        <p>Preparing account…</p>
      )}
    </>
  );
}
