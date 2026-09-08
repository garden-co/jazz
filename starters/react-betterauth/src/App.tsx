import { useSession } from "./auth-client";
import { SignInForm } from "./sign-in-form";
import { TodoWidget } from "./todo-widget";
import { useAuthActions } from "./main";

export function App() {
  const actions = useAuthActions();
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
      await actions.signOut();
    } catch (cause) {
      actions.reportFailure(cause);
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
