"use client";

import Image from "next/image";
import { useActionState } from "react";
import { authClient } from "@/lib/auth-client";

async function signInAction(_prev: string | null, formData: FormData): Promise<string | null> {
  const email = formData.get("email") as string;
  const password = formData.get("password") as string;

  const { error } = await authClient.signIn.email({ email, password });

  if (error) {
    return error.message ?? "Sign-in failed";
  }

  window.location.assign("/");
  return null;
}

export function SignInForm() {
  const [error, formAction, isPending] = useActionState(signInAction, null);

  return (
    <main className="page-center">
      <Image
        src="/jazz.svg"
        alt="Jazz"
        className="wordmark"
        width={80}
        height={24}
        style={{ width: "100%", height: "auto" }}
        loading="eager"
      />
      <div className="card">
        <h1>Sign in</h1>
        <form action={formAction}>
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
            {isPending ? "Signing in…" : "Sign in"}
          </button>
        </form>
      </div>
    </main>
  );
}
