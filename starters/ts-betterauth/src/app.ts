import type { Db } from "jazz-tools";
import { authClient, type AuthSession } from "./auth-client.js";
import { mountTodoWidget } from "./todo-widget.js";
import { mountSignInForm } from "./sign-in-form.js";

export interface AppHandle {
  setDb(db: Db | null): void;
  destroy(): void;
}

export function mountApp(root: HTMLElement, initialDb: Db | null): AppHandle {
  let db = initialDb;
  let unsubscribeTodos: (() => void) | null = null;

  const sessionAtom = authClient.useSession;
  let session: AuthSession = sessionAtom.get();
  const unsubscribeSession = sessionAtom.subscribe((next: AuthSession) => {
    session = next;
    render();
  });

  async function handleSignOut() {
    await authClient.signOut();
    location.assign("/");
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
      mountSignInForm(root.querySelector<HTMLElement>('[data-slot="signin"]')!);
      return;
    }

    if (!db) {
      root.innerHTML = `<div>Loading…</div>`;
      return;
    }

    const name = session.data.user?.name ?? "";
    root.innerHTML = `
      <main class="dashboard">
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
    unsubscribeTodos = mountTodoWidget(root.querySelector<HTMLElement>('[data-slot="todo"]')!, db);
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
