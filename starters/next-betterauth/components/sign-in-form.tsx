"use client";

import { useState, useActionState } from "react";
import { authClient } from "@/lib/auth-client";

async function authAction(_prev: string | null, formData: FormData): Promise<string | null> {
  const email = formData.get("email") as string;
  const password = formData.get("password") as string;
  const name = formData.get("name") as string | null;

  const { error } = await (name
    ? authClient.signUp.email({ name, email, password })
    : authClient.signIn.email({ email, password }));

  if (error) {
    return error.message ?? (name ? "Sign-up failed" : "Sign-in failed");
  }

  window.location.assign("/dashboard");
  return null;
}

export function SignInForm() {
  const [isSignUp, setIsSignUp] = useState(false);
  const [error, formAction, isPending] = useActionState(authAction, null);

  return (
    <div className="card">
      <h1>{isSignUp ? "Create account" : "Sign in"}</h1>
      <form action={formAction}>
        {isSignUp && (
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
          {isSignUp ? "Create account" : "Sign in"}
        </button>
      </form>
      <p className="toggle">
        {isSignUp ? "Already have an account?" : "New here?"}
        <button type="button" className="link" onClick={() => setIsSignUp(!isSignUp)}>
          {isSignUp ? "Sign in" : "Create an account"}
        </button>
      </p>
    </div>
  );
}
