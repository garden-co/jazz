import { useState } from "react";
import { authClient, useSession } from "./auth-client";
import { AuthBackup } from "./auth-backup";
import { SignInForm } from "./sign-in-form";
import { SignUpForm } from "./sign-up-form";
import { TodoWidget } from "./todo-widget";
import { useJazzAuth } from "jazz-tools/react";
import { useProviderError } from "./main";

type View = "dashboard" | "signin" | "signup";

export function App() {
  const lifecycle = useJazzAuth();
  const reportError = useProviderError();
  const { data: session, isPending } = useSession();
  const [view, setView] = useState<View>("dashboard");

  if (isPending) return <div>Loading…</div>;

  async function handleSignOut() {
    reportError(undefined);
    try {
      await lifecycle.sessionActions.logout();
      const result = await authClient.signOut();
      if (result.error) throw new Error(result.error.message ?? "Provider sign-out failed");
      await lifecycle.sessionActions.createLocalFirst();
      setView("dashboard");
    } catch (cause) {
      reportError(cause instanceof Error ? cause : new Error(String(cause)));
    }
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
