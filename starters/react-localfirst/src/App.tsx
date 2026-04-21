import { TodoWidget } from "./todo-widget";
import { AuthBackup } from "./auth-backup";

export function App() {
  return (
    <main className="dashboard">
      <header>
        <img src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
      </header>
      <TodoWidget />
      <AuthBackup />
    </main>
  );
}
