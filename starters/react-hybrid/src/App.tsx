import { useState } from "react";
import { BrowserAuthSecretStore } from "jazz-tools";
import { authClient, useSession } from "./auth-client";
import { AuthBackup } from "./auth-backup";
import { SignInForm } from "./sign-in-form";
import { SignUpForm } from "./sign-up-form";
import { TodoWidget } from "./todo-widget";

type View = "dashboard" | "signin" | "signup";

export function App() {
  const { data: session, isPending } = useSession();
  const [view, setView] = useState<View>("dashboard");

  if (isPending) return <div>Loading…</div>;

  async function handleSignOut() {
    await BrowserAuthSecretStore.clearSecret();
    await authClient.signOut();
    setView("dashboard");
  }

  if (!session && view === "signup") {
    return (
      <main className="page-center">
        <img src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
        <SignUpForm onToggle={() => setView("signin")} />
      </main>
    );
  }

  if (!session && view === "signin") {
    return (
      <main className="page-center">
        <img src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
        <SignInForm onToggle={() => setView("signup")} />
      </main>
    );
  }

  return (
    <main className="dashboard">
      <header>
        <img src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
        <div className="auth-nav">
          {session ? (
            <>
              <p>Hello, {session.user.name}</p>
              <button type="button" className="btn-secondary" onClick={handleSignOut}>
                Sign out
              </button>
            </>
          ) : (
            <p>
              <button type="button" className="link" onClick={() => setView("signup")}>
                Sign up
              </button>
              {" or "}
              <button type="button" className="link" onClick={() => setView("signin")}>
                Sign in
              </button>
            </p>
          )}
        </div>
      </header>
      <TodoWidget />
      {!session && <AuthBackup />}
    </main>
  );
}
