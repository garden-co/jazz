import type { Db } from "jazz-tools";
import { BrowserAuthSecretStore } from "jazz-tools";
import { authClient, type AuthSession } from "./auth-client.js";
import { mountTodoWidget } from "./todo-widget.js";
import { mountAuthBackup } from "./auth-backup.js";
import { mountSignInForm } from "./sign-in-form.js";
import { mountSignUpForm } from "./sign-up-form.js";

type View = "dashboard" | "signin" | "signup";

export interface AppHandle {
  setDb(db: Db): void;
  destroy(): void;
}

export function mountApp(root: HTMLElement, initialDb: Db): AppHandle {
  let db = initialDb;
  let view: View = "dashboard";
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
    await BrowserAuthSecretStore.clearSecret();
    await authClient.signOut();
    setView("dashboard");
  }

  function render() {
    unsubscribeTodos?.();
    unsubscribeTodos = null;

    if (session.isPending) {
      root.innerHTML = `<div>Loading…</div>`;
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
      mountSignUpForm(root.querySelector<HTMLElement>('[data-slot="signup"]')!, db, () =>
        setView("signin"),
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
      mountSignInForm(root.querySelector<HTMLElement>('[data-slot="signin"]')!, () =>
        setView("signup"),
      );
      return;
    }

    const name = session.data?.user?.name ?? "";
    root.innerHTML = `
      <main class="dashboard">
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
        <section data-slot="todo"></section>
        ${signedIn ? "" : `<section data-slot="auth-backup"></section>`}
      </main>
    `;

    root.querySelector('[data-action="signout"]')?.addEventListener("click", handleSignOut);
    root
      .querySelector('[data-action="signup"]')
      ?.addEventListener("click", () => setView("signup"));
    root
      .querySelector('[data-action="signin"]')
      ?.addEventListener("click", () => setView("signin"));

    unsubscribeTodos = mountTodoWidget(root.querySelector<HTMLElement>('[data-slot="todo"]')!, db);

    const authBackupSlot = root.querySelector<HTMLElement>('[data-slot="auth-backup"]');
    if (authBackupSlot) mountAuthBackup(authBackupSlot);
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
