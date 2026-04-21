import { authClient, useSession } from "./auth-client";
import { SignInForm } from "./sign-in-form";
import { TodoWidget } from "./todo-widget";

export function App() {
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
    await authClient.signOut();
    window.location.assign("/");
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
