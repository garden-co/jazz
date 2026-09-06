import type { Db } from "jazz-tools";
import { authClient, type AuthSession } from "./auth-client.js";
import { mountTodoWidget } from "./todo-widget.js";
import { mountSignInForm } from "./sign-in-form.js";
import type { JazzLifecycle } from "./jazz-lifecycle.js";
import { getToken } from "./accounts.js";

export interface AppHandle {
  setDb(db: Db | null): void;
  destroy(): void;
}

export type Authenticate = (
  enroll: boolean,
  request: () => Promise<{ error?: { message?: string | null } | null }>,
) => Promise<void>;

export function mountApp(
  root: HTMLElement,
  lifecycle: JazzLifecycle,
  initialRegistrationError?: Error,
): AppHandle {
  let db: Db | null = lifecycle.getClient() ?? null;
  let registrationError = initialRegistrationError;
  let recovery: "login" | "register" = initialRegistrationError ? "login" : "register";
  let actionError: Error | undefined;
  let sessionVersion = 0;
  let handledSession = sessionKey(authClient.useSession.get());
  let admittedSession = db ? handledSession : null;
  let explicitAuth = false;
  let unsubscribeTodos: (() => void) | null = null;

  const sessionAtom = authClient.useSession;
  let session: AuthSession = sessionAtom.get();
  const unsubscribeSession = sessionAtom.subscribe((next: AuthSession) => {
    session = next;
    reconcile(next);
    render();
  });

  function reconcile(next: AuthSession) {
    const key = sessionKey(next);
    if (explicitAuth || key === handledSession) return;
    handledSession = key;
    admittedSession = null;
    const version = ++sessionVersion;
    void lifecycle
      .transition(
        async (manager) => {
          if (key) await manager.loginJWT({ getToken });
          else manager.logout();
        },
        () => !explicitAuth && version === sessionVersion,
        false,
      )
      .then(() => {
        if (version === sessionVersion) {
          registrationError = undefined;
          admittedSession = key;
        }
      })
      .catch((cause) => {
        if (version === sessionVersion) {
          recovery = "login";
          registrationError = cause instanceof Error ? cause : new Error(String(cause));
        }
      })
      .finally(render);
  }

  const authenticate: Authenticate = async (enroll, request) => {
    explicitAuth = true;
    const version = ++sessionVersion;
    try {
      const result = await request();
      if (result.error)
        throw new Error(result.error.message ?? (enroll ? "Sign-up failed" : "Sign-in failed"));
      const current = await authClient.getSession();
      handledSession = sessionKey({ data: current.data });
      await lifecycle.transition(
        (manager) => (enroll ? manager.registerJWT({ getToken }) : manager.loginJWT({ getToken })),
        () => explicitAuth && version === sessionVersion,
      );
      registrationError = undefined;
      admittedSession = handledSession;
    } catch (cause) {
      if (sessionKey(sessionAtom.get())) {
        recovery = enroll ? "register" : "login";
        registrationError = cause instanceof Error ? cause : new Error(String(cause));
      }
      throw cause;
    } finally {
      explicitAuth = false;
      reconcile(sessionAtom.get());
      render();
    }
  };

  async function handleSignOut() {
    try {
      await lifecycle.transition(async (manager) => {
        await authClient.signOut();
        manager.logout();
      });
      location.assign("/");
    } catch (cause) {
      actionError = cause instanceof Error ? cause : new Error(String(cause));
      render();
    }
  }

  async function register() {
    const version = ++sessionVersion;
    const recoveryKey = sessionKey(session);
    try {
      await lifecycle.transition(
        (manager) =>
          recovery === "login" ? manager.loginJWT({ getToken }) : manager.registerJWT({ getToken }),
        () => version === sessionVersion && sessionKey(session) === recoveryKey,
        recovery !== "login",
      );
      if (version === sessionVersion && sessionKey(session) === recoveryKey)
        registrationError = undefined;
    } catch (cause) {
      if (version === sessionVersion && sessionKey(session) === recoveryKey)
        registrationError = cause instanceof Error ? cause : new Error(String(cause));
    }
    render();
  }

  function render() {
    unsubscribeTodos?.();
    unsubscribeTodos = null;

    if (session.isPending) {
      root.innerHTML = `<div>Loading…</div>`;
      return;
    }

    if (!session.data?.session) {
      root.innerHTML = `
        <main class="page-center">
          <img src="/jazz.svg" alt="Jazz" class="wordmark" width="80" height="24" />
          <div data-slot="signin"></div>
        </main>
      `;
      mountSignInForm(root.querySelector<HTMLElement>('[data-slot="signin"]')!, authenticate);
      return;
    }

    if (!db || admittedSession !== sessionKey(session)) {
      root.innerHTML = registrationError
        ? `<main class="page-center"><div class="card"><p class="alert-error" role="alert">${escapeHtml(registrationError.message)}</p><p>Finish setting up your Jazz account.</p><button type="button" class="btn-primary" data-action="register">${recovery === "login" ? "Retry sign in" : "Complete account setup"}</button></div></main>`
        : `<div>Loading…</div>`;
      root.querySelector('[data-action="register"]')?.addEventListener("click", register);
      return;
    }

    const todoDb = db;
    const name = session.data.user?.name ?? "";
    root.innerHTML = `
      <main class="dashboard">
        ${actionError ? `<aside class="alert-error" role="alert">${escapeHtml(actionError.message)} Retry sign out when syncing is available.</aside>` : ""}
        <header>
          <img src="/jazz.svg" alt="Jazz" class="wordmark" width="80" height="24" />
          <div class="auth-nav">
            <p>Hello, ${escapeHtml(name)}</p>
            <button type="button" data-action="signout">Sign out</button>
          </div>
        </header>
        <section data-slot="todo"></section>
      </main>
    `;
    root.querySelector('[data-action="signout"]')?.addEventListener("click", handleSignOut);
    unsubscribeTodos = mountTodoWidget(
      root.querySelector<HTMLElement>('[data-slot="todo"]')!,
      todoDb,
    );
  }

  render();

  return {
    setDb(next) {
      db = next;
      render();
    },
    destroy() {
      unsubscribeTodos?.();
      unsubscribeSession();
    },
  };
}

function sessionKey(next: {
  data?: { session?: { id?: string } | null; user?: { id?: string } | null } | null;
}): string | null {
  return next.data?.session?.id ?? next.data?.user?.id ?? null;
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
