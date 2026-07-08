import type { QueryBuilder, QueryOptions, SubscriptionDelta } from "jazz-tools/client";
import { mountTodoWidget, type TodoDb } from "./todo-widget.js";
import { mountAuthBackup } from "./auth-backup.js";

export interface TodoRuntime {
  db: TodoDb;
  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    options?: QueryOptions,
  ): () => void;
}

export function mountApp(root: HTMLElement, runtime: TodoRuntime): void {
  root.innerHTML = `
    <main class="dashboard">
      <header>
        <img src="/jazz.svg" alt="Jazz" class="wordmark" width="80" height="24" />
      </header>
      <section data-slot="todo"></section>
      <section data-slot="auth-backup"></section>
    </main>
  `;
  mountTodoWidget(
    root.querySelector<HTMLElement>('[data-slot="todo"]')!,
    runtime.db,
    runtime.subscribeAll,
  );
  mountAuthBackup(root.querySelector<HTMLElement>('[data-slot="auth-backup"]')!);
}
