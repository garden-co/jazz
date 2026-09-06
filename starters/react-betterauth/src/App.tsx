import { authClient, useSession } from "./auth-client";
import { SignInForm } from "./sign-in-form";
import { TodoWidget } from "./todo-widget";
import { useJazzLifecycle } from "./main";

export function App() {
  const lifecycle = useJazzLifecycle();
  const { data: session, isPending } = useSession();
  if (isPending) return <div>Loading…</div>;

  if (!session) {
    return (
      <main className="page-center">
        <img src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
        <SignInForm />
      </main>
    );
  }

  async function handleSignOut() {
    try {
      await lifecycle.transition(async (accounts) => {
        await authClient.signOut();
        accounts.logout();
      });
      window.location.assign("/");
    } catch (cause) {
      lifecycle.reportFailure(cause);
    }
  }

  return (
    <main className="dashboard">
      <header>
        <img src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
        <div className="auth-nav">
          <p>Hello, {session.user.name}</p>
          <button type="button" onClick={handleSignOut}>
            Sign out
          </button>
        </div>
      </header>
      <TodoWidget />
    </main>
  );
}
