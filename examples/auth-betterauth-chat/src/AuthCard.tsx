import * as React from "react";
import { useSession } from "jazz-tools/react";

export type AuthCardProps = {
  loggedIn: boolean;
  role?: string | null;
  onSignIn: (email: string, password: string) => Promise<void>;
  onSignUp: (email: string, password: string) => Promise<void>;
  onSignOut: () => void;
};

type AuthMode = "signin" | "signup";

export function AuthCard({ loggedIn, role, onSignIn, onSignUp, onSignOut }: AuthCardProps) {
  const session = useSession();
  const [mode, setMode] = React.useState<AuthMode>("signin");
  const [email, setEmail] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [pending, setPending] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  function switchMode(next: AuthMode) {
    setMode(next);
    setError(null);
    setPassword("");
  }

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!email.trim() || !password) {
      setError("Email and password are required.");
      return;
    }

    setPending(true);
    setError(null);

    try {
      if (mode === "signin") {
        await onSignIn(email.trim(), password);
      } else {
        await onSignUp(email.trim(), password);
      }
      setPassword("");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setPending(false);
    }
  }

  function handleSignOut() {
    setPassword("");
    setError(null);
    onSignOut();
  }

  const isSignIn = mode === "signin";

  return (
    <aside className="auth-card">
      <div className="status-card" data-testid="auth-status">
        <div className="status-copy">
          <span className="status-name">{loggedIn ? session?.user_id : "Anonymous"}</span>
        </div>
        {role ? <span className="admin-badge">{role}</span> : null}
      </div>

      {loggedIn ? (
        <div className="signed-in-card">
          <button type="button" data-testid="logout-button" onClick={handleSignOut}>
            Log out
          </button>
        </div>
      ) : (
        <>
          <div className="auth-tabs">
            <button
              type="button"
              className={isSignIn ? "auth-tab active" : "auth-tab"}
              onClick={() => switchMode("signin")}
            >
              Sign in
            </button>
            <button
              type="button"
              className={!isSignIn ? "auth-tab active" : "auth-tab"}
              onClick={() => switchMode("signup")}
            >
              Sign up
            </button>
          </div>

          <form className="auth-form" onSubmit={handleSubmit}>
            <label>
              Email
              <input
                type="email"
                name="email"
                autoComplete="email"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
              />
            </label>
            <label>
              Password
              <input
                type="password"
                name="password"
                autoComplete={isSignIn ? "current-password" : "new-password"}
                value={password}
                onChange={(event) => setPassword(event.target.value)}
              />
            </label>
            <button type="submit" data-testid="auth-submit" disabled={pending}>
              {pending
                ? isSignIn
                  ? "Signing in..."
                  : "Creating account..."
                : isSignIn
                  ? "Login"
                  : "Create account"}
            </button>
            {error ? (
              <p className="error-text" data-testid="auth-error">
                {error}
              </p>
            ) : null}

            <p className="helper-text">
              Log in as <code>admin@example.com / admin</code>, or create a new account.
            </p>
          </form>
        </>
      )}
    </aside>
  );
}
