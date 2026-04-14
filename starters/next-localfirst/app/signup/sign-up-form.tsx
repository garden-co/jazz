"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Image from "next/image";
import { useDb } from "jazz-tools/react";
import { authClient } from "@/src/lib/auth-client";

export function SignUpForm() {
  const router = useRouter();
  const db = useDb();
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);

    try {
      const proofToken = await db.getLocalFirstIdentityProof({
        ttlSeconds: 60,
        audience: "next-localfirst-signup",
      });

      if (!proofToken) {
        setError("Sign up requires an active Jazz session");
        return;
      }

      const res = await authClient.signUp.email({
        email,
        name,
        password,
        proofToken,
      } as Parameters<typeof authClient.signUp.email>[0]);

      if (res.error) {
        setError(res.error.message ?? "Sign-up failed");
        return;
      }

      router.push("/");
      router.refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Sign-up failed");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main className="page-center">
      <Image src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
      <div className="card">
        <h1>Create account</h1>
        <form onSubmit={handleSubmit}>
          <div className="field">
            <label htmlFor="name">Name</label>
            <input
              id="name"
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
            />
          </div>
          <div className="field">
            <label htmlFor="email">Email</label>
            <input
              id="email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
            />
          </div>
          <div className="field">
            <label htmlFor="password">Password</label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
          </div>
          {error && (
            <p className="alert-error" role="alert">
              {error}
            </p>
          )}
          <button type="submit" className="btn-primary" disabled={submitting}>
            {submitting ? "Creating account…" : "Create account"}
          </button>
        </form>
      </div>
    </main>
  );
}
