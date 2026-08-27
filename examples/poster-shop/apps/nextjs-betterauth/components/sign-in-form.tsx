"use client";

import { useActionState, useState } from "react";
import { authClient } from "@/src/lib/auth-client";

async function authAction(_previous: string | null, formData: FormData): Promise<string | null> {
  const email = String(formData.get("email") ?? "");
  const password = String(formData.get("password") ?? "");
  const name = formData.get("name");
  const { error } = await (name
    ? authClient.signUp.email({ name: String(name), email, password })
    : authClient.signIn.email({ email, password }));
  if (error) return error.message ?? "Authentication failed";
  window.location.assign("/dashboard");
  return null;
}

export function SignInForm() {
  const [signUp, setSignUp] = useState(false);
  const [error, formAction, pending] = useActionState(authAction, null);
  return (
    <form action={formAction}>
      <h1>{signUp ? "Create account" : "Sign in"}</h1>
      {signUp && (
        <label>
          Name <input name="name" required />
        </label>
      )}
      <label>
        Email <input name="email" type="email" required />
      </label>
      <label>
        Password <input name="password" type="password" required />
      </label>
      {error && <p role="alert">{error}</p>}
      <button disabled={pending} type="submit">
        {signUp ? "Create account" : "Sign in"}
      </button>
      <button type="button" onClick={() => setSignUp(!signUp)}>
        {signUp ? "I have an account" : "Create an account"}
      </button>
    </form>
  );
}
