import { useState } from "react";
import { useDb } from "jazz-tools/react";
import { authClient } from "./auth-client";

export function SignUpForm({ onToggle }: { onToggle: () => void }) {
  const db = useDb();
  const [error, setError] = useState<string | null>(null);
  const [isPending, setIsPending] = useState(false);

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    setIsPending(true);

    const form = e.currentTarget;
    const name = (form.elements.namedItem("name") as HTMLInputElement).value;
    const email = (form.elements.namedItem("email") as HTMLInputElement).value;
    const password = (form.elements.namedItem("password") as HTMLInputElement).value;

    const proofToken = await db.getLocalFirstIdentityProof({
      ttlSeconds: 60,
      audience: "react-localfirst-signup",
    });

    if (!proofToken) {
      setError("Sign up requires an active Jazz session");
      setIsPending(false);
      return;
    }

    const { error: signUpError } = await authClient.signUp.email({
      email,
      name,
      password,
      proofToken,
    } as Parameters<typeof authClient.signUp.email>[0]);

    setIsPending(false);

    if (signUpError) {
      setError(signUpError.message ?? "Sign-up failed");
    }
  }

  return (
    <div className="card">
      <h1>Create account</h1>
      <form onSubmit={handleSubmit}>
        <div className="field">
          <label htmlFor="name">Name</label>
          <input id="name" name="name" type="text" required />
        </div>
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
          {isPending ? "Creating account…" : "Create account"}
        </button>
      </form>
      <p className="toggle">
        Already have an account?
        <button type="button" className="link" onClick={onToggle}>
          Sign in
        </button>
      </p>
    </div>
  );
}
