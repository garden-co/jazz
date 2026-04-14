"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Image from "next/image";
import { authClient } from "@/src/lib/auth-client";

type Mode = "signIn" | "signUp";

export default function HomePage() {
  const router = useRouter();
  const [mode, setMode] = useState<Mode>("signIn");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);

  const isSignUp = mode === "signUp";

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    const res = isSignUp
      ? await authClient.signUp.email({ email, name, password })
      : await authClient.signIn.email({ email, password });
    if (res.error) {
      setError(res.error.message ?? `${isSignUp ? "Sign-up" : "Sign-in"} failed`);
      return;
    }
    router.push("/dashboard");
  }

  return (
    <main className="page-center">
      <Image src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
      <div className="card">
        <h1>{isSignUp ? "Create account" : "Sign in"}</h1>
        <form onSubmit={handleSubmit}>
          {isSignUp && (
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
          )}
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
          <button type="submit" className="btn-primary">
            {isSignUp ? "Create account" : "Sign in"}
          </button>
        </form>
        <p className="toggle">
          {isSignUp ? "Already have an account?" : "New here?"}{" "}
          <button
            type="button"
            className="link"
            onClick={() => {
              setMode(isSignUp ? "signIn" : "signUp");
              setError(null);
            }}
          >
            {isSignUp ? "Sign in" : "Create an account"}
          </button>
        </p>
      </div>
    </main>
  );
}
