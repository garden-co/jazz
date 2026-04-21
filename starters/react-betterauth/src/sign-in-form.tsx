import { useState } from "react";
import { authClient } from "./auth-client";

export function SignInForm() {
  const [mode, setMode] = useState<"signin" | "signup">("signin");
  const [error, setError] = useState<string | null>(null);
  const [isPending, setIsPending] = useState(false);

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    setIsPending(true);

    const form = e.currentTarget;
    const email = (form.elements.namedItem("email") as HTMLInputElement).value;
    const password = (form.elements.namedItem("password") as HTMLInputElement).value;

    const result =
      mode === "signup"
        ? await authClient.signUp.email({
            name: (form.elements.namedItem("name") as HTMLInputElement).value,
            email,
            password,
          })
        : await authClient.signIn.email({ email, password });

    setIsPending(false);

    if (result.error) {
      setError(result.error.message ?? (mode === "signup" ? "Sign-up failed" : "Sign-in failed"));
    }
  }

  return (
    <div className="card">
      <h1>{mode === "signup" ? "Create account" : "Sign in"}</h1>
      <form onSubmit={handleSubmit}>
        {mode === "signup" && (
          <div className="field">
            <label htmlFor="name">Name</label>
            <input id="name" name="name" type="text" required />
          </div>
        )}
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
          {mode === "signup" ? "Create account" : "Sign in"}
        </button>
      </form>
      <p className="toggle">
        {mode === "signup" ? "Already have an account?" : "New here?"}
        <button
          type="button"
          className="link"
          onClick={() => {
            setMode(mode === "signup" ? "signin" : "signup");
            setError(null);
          }}
        >
          {mode === "signup" ? "Sign in" : "Create an account"}
        </button>
      </p>
    </div>
  );
}
