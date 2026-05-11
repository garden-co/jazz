import type { Db } from "jazz-tools";
import { mountTodoWidget } from "./todo-widget.js";
import { mountAuthBackup } from "./auth-backup.js";

export function mountApp(root: HTMLElement, db: Db): void {
  root.innerHTML = `
    <main class="dashboard">
      <header>
        <img src="/jazz.svg" alt="Jazz" class="wordmark" width="80" height="24" />
      </header>
      <section data-slot="todo"></section>
      <section data-slot="auth-backup"></section>
    </main>
  `;
  mountTodoWidget(root.querySelector<HTMLElement>('[data-slot="todo"]')!, db);
  mountAuthBackup(root.querySelector<HTMLElement>('[data-slot="auth-backup"]')!);
}
