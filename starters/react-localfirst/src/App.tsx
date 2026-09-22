import { TodoWidget } from "./todo-widget";
import { AuthBackup } from "./auth-backup";
import type { AccountHandle } from "jazz-tools";

export function App({
  account,
  onRestore,
}: {
  account: AccountHandle;
  onRestore: (secret: string) => Promise<void>;
}) {
  return (
    <main className="dashboard">
      <header>
        <img src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
      </header>
      <TodoWidget />
      <AuthBackup account={account} onRestore={onRestore} />
    </main>
  );
}
