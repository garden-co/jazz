"use client";

import Image from "next/image";
import { useActionState } from "react";
import { authClient } from "@/lib/auth-client";
import { getToken } from "@/lib/accounts";
import { useJazzAuth } from "jazz-tools/react";

export function SignUpForm() {
  const lifecycle = useJazzAuth();

  async function signUpAction(_prev: string | null, formData: FormData): Promise<string | null> {
    const name = formData.get("name") as string;
    const email = formData.get("email") as string;
    const password = formData.get("password") as string;

    const { error } = await authClient.signUp.email({
      email,
      name,
      password,
    } as Parameters<typeof authClient.signUp.email>[0]);

    if (error) {
      return error.message ?? "Sign-up failed";
    }
    try {
      await lifecycle.sessionActions.linkJWT({ getToken });
    } catch (cause) {
      return cause instanceof Error ? cause.message : "Sign-up failed";
    }

    window.location.assign("/");
    return null;
  }

  const [error, formAction, isPending] = useActionState(signUpAction, null);

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
        <h1>Create account</h1>
        <form action={formAction}>
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
      </div>
    </main>
  );
}
