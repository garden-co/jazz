"use client";

import { useActionState, useState } from "react";
import { authClient } from "@/src/lib/auth-client";

async function authenticate(_previous: string | null, formData: FormData): Promise<string | null> {
  const email = String(formData.get("email") ?? "");
  const password = String(formData.get("password") ?? "");
  const name = formData.get("name");
  const result = name
    ? await authClient.signUp.email({ name: String(name), email, password })
    : await authClient.signIn.email({ email, password });
  if (result.error) return result.error.message ?? "Authentication failed";
  window.location.assign("/dashboard");
  return null;
}

export function SignInForm() {
  const [signingUp, setSigningUp] = useState(false);
  const [error, formAction, pending] = useActionState(authenticate, null);
  return (
    <section className="auth-card">
      <h2>{signingUp ? "Create account" : "Sign in"}</h2>
      <form action={formAction}>
        {signingUp ? (
          <label>
            Name
            <input name="name" required type="text" />
          </label>
        ) : null}
        <label>
          Email
          <input name="email" required type="email" />
        </label>
        <label>
          Password
          <input name="password" required type="password" />
        </label>
        {error ? <p role="alert">{error}</p> : null}
        <button disabled={pending} type="submit">
          {signingUp ? "Create account" : "Sign in"}
        </button>
      </form>
      <button className="link" onClick={() => setSigningUp(!signingUp)} type="button">
        {signingUp ? "Already have an account? Sign in" : "New here? Create an account"}
      </button>
    </section>
  );
}
