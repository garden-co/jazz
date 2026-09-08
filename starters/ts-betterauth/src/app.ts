import type { Db } from "jazz-tools";
import { connectBetterAuth, type createJazzSession } from "jazz-tools/client";
import { authClient } from "./auth-client.js";
import { mountTodoWidget } from "./todo-widget.js";
import { mountSignInForm } from "./sign-in-form.js";
type Session = Awaited<ReturnType<typeof createJazzSession>>;
export interface AppHandle {
  setDb(db: Db | null): void;
  destroy(): void;
}
export function mountApp(root: HTMLElement, jazz: Session): AppHandle {
  const auth = connectBetterAuth(jazz, authClient);
  let db: Db | null = jazz.getSnapshot().client?.db ?? null;
  let session = authClient.useSession.get();
  let unsubscribeTodos: (() => void) | null = null;
  function render() {
    unsubscribeTodos?.();
    unsubscribeTodos = null;

    const error = auth.getSnapshot().error;
    if (error) {
      root.innerHTML = `<main class="page-center"><div class="card"><p class="alert-error" role="alert">${escapeHtml(error.message)}</p><button type="button" class="btn-primary" data-action="retry">Retry</button></div></main>`;
      root
        .querySelector('[data-action="retry"]')
        ?.addEventListener("click", () => void auth.retry().catch(() => {}));
      return;
    }

    if (auth.getSnapshot().isPending) {
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

    if (!db || !auth.getSnapshot().ready) {
      root.innerHTML = `<div>Loading…</div>`;
      return;
    }

    const todoDb = db;
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
    root
      .querySelector('[data-action="signout"]')
      ?.addEventListener("click", () => void auth.logout().catch(() => {}));
    unsubscribeTodos = mountTodoWidget(
      root.querySelector<HTMLElement>('[data-slot="todo"]')!,
      todoDb,
    );
  }

  const unsubscribeAuth = auth.subscribe(render);
  const unsubscribeSession = authClient.useSession.subscribe((next) => {
    session = next;
    render();
  });
  render();
  return {
    setDb(next) {
      db = next;
      render();
    },
    destroy() {
      unsubscribeTodos?.();
      unsubscribeAuth();
      unsubscribeSession();
      auth.dispose();
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
