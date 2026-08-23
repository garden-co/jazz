"use client";
import { useActionState, useState } from "react";
import { authClient } from "@/src/lib/auth-client";

async function authenticate(_previous: string | null, data: FormData): Promise<string | null> {
  const email = String(data.get("email"));
  const password = String(data.get("password"));
  const name = data.get("name");
  const result = name
    ? await authClient.signUp.email({ name: String(name), email, password })
    : await authClient.signIn.email({ email, password });
  if (result.error) return result.error.message ?? "Authentication failed";
  window.location.assign("/dashboard");
  return null;
}

export function SignInForm() {
  const [signUp, setSignUp] = useState(false);
  const [error, action, pending] = useActionState(authenticate, null);
  return (
    <section className="empty">
      <form action={action}>
        {signUp && <input aria-label="Display name" name="name" required />}
        <input aria-label="Email" name="email" type="email" required />
        <input aria-label="Password" name="password" type="password" minLength={8} required />
        <button type="submit" disabled={pending}>
          {signUp ? "Create account" : "Sign in"}
        </button>
      </form>
      {error && <p role="alert">{error}</p>}
      <button type="button" onClick={() => setSignUp((value) => !value)}>
        {signUp ? "Use an existing account" : "Create an account"}
      </button>
    </section>
  );
}
