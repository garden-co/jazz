import { useState } from "react";
import { authClient } from "./auth-client";
import { getToken } from "./accounts";
import { useJazzAuth } from "jazz-tools/react";

export function SignInForm({ onToggle }: { onToggle: () => void }) {
  const lifecycle = useJazzAuth();
  const [error, setError] = useState<string | null>(null);
  const [isPending, setIsPending] = useState(false);

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    setIsPending(true);

    const form = e.currentTarget;
    const email = (form.elements.namedItem("email") as HTMLInputElement).value;
    const password = (form.elements.namedItem("password") as HTMLInputElement).value;

    const result = await authClient.signIn.email({ email, password });

    if (result.error) {
      setError(result.error.message ?? "Sign-in failed");
      setIsPending(false);
      return;
    }
    try {
      await lifecycle.sessionActions.loginOrRegisterJWT({ getToken });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Sign-in failed");
    } finally {
      setIsPending(false);
    }
  }

  return (
    <div className="card">
      <h1>Sign in</h1>
      <form onSubmit={handleSubmit}>
        <div className="field">
          <label htmlFor="email">Email</label>
          <input id="email" name="email" type="email" required />
        </div>
        <div className="field">
          <label htmlFor="password">Password</label>
          <input id="password" name="password" type="password" required />
        </div>
        {error && (
          <p className="alert-error" role="alert">
            {error}
          </p>
        )}
        <button type="submit" className="btn-primary" disabled={isPending}>
          Sign in
        </button>
      </form>
      <p className="toggle">
        New here?
        <button type="button" className="link" onClick={onToggle}>
          Create an account
        </button>
      </p>
    </div>
  );
}
