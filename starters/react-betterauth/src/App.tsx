import { useSession } from "./auth-client";
import { TodoWidget } from "./todo-widget";
import { useJazzAuth } from "jazz-tools/react";

export function App() {
  const { logout } = useJazzAuth();
  const { data: session } = useSession();
  if (!session) return null;

  return (
    <main className="dashboard">
      <header>
        <img src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
        <div className="auth-nav">
          <p>Hello, {session.user.name}</p>
          <button type="button" onClick={() => void logout()}>
            Sign out
          </button>
        </div>
      </header>
      <TodoWidget />
    </main>
  );
}
