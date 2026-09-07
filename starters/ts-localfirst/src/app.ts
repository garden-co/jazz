import { mountTodoWidget, type TodoDb } from "./todo-widget.js";
import { mountAuthBackup } from "./auth-backup.js";
import type { AccountHandle } from "jazz-tools";

export function mountApp(
  root: HTMLElement,
  db: TodoDb,
  account: AccountHandle,
  onRestore: (secret: string) => Promise<void>,
): () => void {
  root.innerHTML = `
    <main class="dashboard">
      <header>
        <img src="/jazz.svg" alt="Jazz" class="wordmark" width="80" height="24" />
      </header>
      <section data-slot="todo"></section>
      <section data-slot="auth-backup"></section>
    </main>
  `;
  const unsubscribe = mountTodoWidget(root.querySelector<HTMLElement>('[data-slot="todo"]')!, db);
  mountAuthBackup(root.querySelector<HTMLElement>('[data-slot="auth-backup"]')!, {
    account,
    onRestore,
  });
  return unsubscribe;
}
