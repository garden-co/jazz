import type { Db } from "jazz-tools";
import type { JazzApp } from "jazz-tools/client";
import { authClient } from "./auth-client.js";
import { mountTodoWidget } from "./todo-widget.js";
import { mountSignInForm } from "./sign-in-form.js";
type App = JazzApp<{ db: Db }>;
export interface AppHandle {
  destroy(): void;
}
export function mountApp(root: HTMLElement, jazz: App): AppHandle {
  const consumer = jazz.attachConsumer();
  let unsubscribeTodos: (() => void) | null = null;
  function render() {
    unsubscribeTodos?.();
    unsubscribeTodos = null;

    const snapshot = jazz.getSnapshot();
    consumer.acknowledge(snapshot);
    const error = snapshot.error;
    if (error) {
      root.innerHTML = `<main class="page-center"><div class="card"><p class="alert-error" role="alert">${escapeHtml(error.message)}</p><button type="button" class="btn-primary" data-action="retry">Retry</button></div></main>`;
      root
        .querySelector('[data-action="retry"]')
        ?.addEventListener("click", () => void jazz.retry().catch(() => {}));
      return;
    }

    if (snapshot.status === "starting" || snapshot.status === "transitioning") {
      root.innerHTML = `<div>Loading…</div>`;
      return;
    }

    if (snapshot.status === "signed-out") {
      root.innerHTML = `
        <main class="page-center">
          <img src="/jazz.svg" alt="Jazz" class="wordmark" width="80" height="24" />
          <div data-slot="signin"></div>
        </main>
      `;
      mountSignInForm(root.querySelector<HTMLElement>('[data-slot="signin"]')!);
      return;
    }

    if (snapshot.status !== "ready" || !snapshot.client) {
      root.innerHTML = `<div>Loading…</div>`;
      return;
    }

    const todoDb = snapshot.client.db;
    const name = authClient.useSession.get().data?.user?.name ?? "";
    root.innerHTML = `
      <main class="dashboard">
        <header>
          <img src="/jazz.svg" alt="Jazz" class="wordmark" width="80" height="24" />
          <div class="auth-nav">
            <p data-slot="profile">Hello, ${escapeHtml(name)}</p>
            <button type="button" data-action="signout">Sign out</button>
          </div>
        </header>
        <section data-slot="todo"></section>
      </main>
    `;
    root
      .querySelector('[data-action="signout"]')
      ?.addEventListener("click", () => void jazz.logout().catch(() => {}));
    unsubscribeTodos = mountTodoWidget(
      root.querySelector<HTMLElement>('[data-slot="todo"]')!,
      todoDb,
    );
  }

  const unsubscribe = jazz.subscribe(render);
  // Profile changes are presentation-only; Jazz owns identity admission and lifecycle.
  const unsubscribeProfile = authClient.useSession.subscribe((session) => {
    const profile = root.querySelector('[data-slot="profile"]');
    if (profile) profile.textContent = `Hello, ${session.data?.user?.name ?? ""}`;
  });
  render();
  return {
    destroy() {
      unsubscribe();
      unsubscribeProfile();
      unsubscribeTodos?.();
      unsubscribeTodos = null;
      root.replaceChildren();
      consumer.release();
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
