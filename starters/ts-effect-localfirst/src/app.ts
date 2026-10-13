import type { AccountHandle, Db } from "jazz-tools";
import { Jazz } from "jazz-tools/effect";
import { mountTodoWidget } from "./todo-widget.js";
import { mountAuthBackup } from "./auth-backup.js";

export function mountApp(
  root: HTMLElement,
  db: Db,
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
  // `Jazz.fromDb` exposes the session's database as the Effect `Jazz` service.
  const unsubscribe = mountTodoWidget(
    root.querySelector<HTMLElement>('[data-slot="todo"]')!,
    Jazz.fromDb(db),
  );
  mountAuthBackup(root.querySelector<HTMLElement>('[data-slot="auth-backup"]')!, {
    account,
    onRestore,
  });
  return unsubscribe;
}
