import type { Db } from "jazz-tools";
import { authClient, type AuthSession } from "./auth-client.js";
import { mountTodoWidget } from "./todo-widget.js";
import { mountAuthBackup } from "./auth-backup.js";
import { mountSignInForm } from "./sign-in-form.js";
import { mountSignUpForm } from "./sign-up-form.js";
import type { createJazzSession } from "jazz-tools/client";
type Session = Awaited<ReturnType<typeof createJazzSession>>;
import { getToken } from "./accounts.js";

type View = "dashboard" | "signin" | "signup";

export interface AppHandle {
  setDb(db: Db | undefined): void;
  destroy(): void;
}

export function mountApp(
  root: HTMLElement,
  initialDb: Db,
  jazz: Session,
  initialProviderLinkError?: Error,
): AppHandle {
  let db: Db | undefined = initialDb;
  let view: View = "dashboard";
  let providerLinkError = initialProviderLinkError;
  let providerError: Error | undefined;
  let unsubscribeTodos: (() => void) | null = null;

  const sessionAtom = authClient.useSession;
  let session: AuthSession = sessionAtom.get();
  const unsubscribeSession = sessionAtom.subscribe((next: AuthSession) => {
    session = next;
    render();
  });

  function setView(next: View) {
    view = next;
    render();
  }

  async function handleSignOut() {
    providerError = undefined;
    try {
      await jazz.logout();
      const result = await authClient.signOut();
      if (result.error) throw new Error(result.error.message ?? "Provider sign-out failed");
      await jazz.createLocalFirst();
      setView("dashboard");
    } catch (cause) {
      providerError = cause instanceof Error ? cause : new Error(String(cause));
      render();
    }
  }

  function reportProviderLinkFailure(cause: unknown) {
    providerLinkError = cause instanceof Error ? cause : new Error(String(cause));
    render();
  }

  async function retryLink() {
    try {
      await jazz.linkJWT({ getToken });
      providerLinkError = undefined;
    } catch (cause) {
      providerLinkError = cause instanceof Error ? cause : new Error(String(cause));
    }
    render();
  }

  async function recover(operation: () => Promise<void>) {
    const failed = providerError;
    await operation();
    if (providerError === failed) providerError = undefined;
    render();
  }

  function render() {
    unsubscribeTodos?.();
    unsubscribeTodos = null;

    if (session.isPending) {
      root.innerHTML = `<div>Loading…</div>`;
      return;
    }

    const sessionError = jazz.getSnapshot().error ?? providerError;
    if (!db) {
      root.innerHTML = sessionError
        ? `<p role="alert">${escapeHtml(sessionError.message)}</p><button data-action="retry-session">Retry Jazz startup</button><button data-action="retry-login">Retry sign in</button><button data-action="continue-local">Continue locally</button>`
        : `<div>Loading…</div>`;
      root.querySelector('[data-action="retry-login"]')?.addEventListener("click", () => {
        void recover(() => jazz.loginJWT({ getToken })).catch(() => {});
      });
      root.querySelector('[data-action="continue-local"]')?.addEventListener("click", () => {
        void recover(jazz.createLocalFirst).catch(() => {});
      });
      root.querySelector('[data-action="retry-session"]')?.addEventListener("click", () => {
        void recover(jazz.retry).catch(() => {});
      });
      return;
    }

    const signedIn = Boolean(session.data?.session);

    if (!signedIn && view === "signup") {
      root.innerHTML = `
        <main class="page-center">
          <img src="/jazz.svg" alt="Jazz" class="wordmark" width="80" height="24" />
          <div data-slot="signup"></div>
        </main>
      `;
      mountSignUpForm(
        root.querySelector<HTMLElement>('[data-slot="signup"]')!,
        jazz,
        () => setView("signin"),
        reportProviderLinkFailure,
      );
      return;
    }

    if (!signedIn && view === "signin") {
      root.innerHTML = `
        <main class="page-center">
          <img src="/jazz.svg" alt="Jazz" class="wordmark" width="80" height="24" />
          <div data-slot="signin"></div>
        </main>
      `;
      mountSignInForm(root.querySelector<HTMLElement>('[data-slot="signin"]')!, jazz, () =>
        setView("signup"),
      );
      return;
    }

    const name = session.data?.user?.name ?? "";
    root.innerHTML = `
      <main class="dashboard">
        ${sessionError ? `<p role="alert">${escapeHtml(sessionError.message)}</p>` : ""}
        <header>
          <img src="/jazz.svg" alt="Jazz" class="wordmark" width="80" height="24" />
          <div class="auth-nav">
            ${
              signedIn
                ? `<p>Hello, ${escapeHtml(name)}</p>
                   <button type="button" class="btn-secondary" data-action="signout">Sign out</button>`
                : `<p>
                     <button type="button" class="link" data-action="signup">Sign up</button>
                     or
                     <button type="button" class="link" data-action="signin">Sign in</button>
                   </p>`
            }
          </div>
        </header>
        ${
          providerLinkError
            ? `<aside class="alert-error" role="alert">
                 Your signed-in account has not been linked to this local data yet. ${escapeHtml(providerLinkError.message)}
                 <button type="button" data-action="retry-link">Retry linking</button>
               </aside>`
            : ""
        }
        <section data-slot="todo"></section>
        ${signedIn ? "" : `<section data-slot="auth-backup"></section>`}
      </main>
    `;

    root.querySelector('[data-action="signout"]')?.addEventListener("click", handleSignOut);
    root.querySelector('[data-action="retry-link"]')?.addEventListener("click", retryLink);
    root
      .querySelector('[data-action="signup"]')
      ?.addEventListener("click", () => setView("signup"));
    root
      .querySelector('[data-action="signin"]')
      ?.addEventListener("click", () => setView("signin"));

    unsubscribeTodos = mountTodoWidget(root.querySelector<HTMLElement>('[data-slot="todo"]')!, db);

    const authBackupSlot = root.querySelector<HTMLElement>('[data-slot="auth-backup"]');
    if (authBackupSlot) mountAuthBackup(authBackupSlot, jazz);
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

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
