"use client";
import { useState } from "react";
import { useJazzSession } from "jazz-tools/react";
import { authClient } from "@/lib/auth-client";

export function AuthForm() {
  const session = useJazzSession();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [msg, setMsg] = useState("");

  async function signUp() {
    const r = await authClient.signUp.email({ email, password, name: email.split("@")[0] });
    if (r.error) return setMsg("sign-up error: " + JSON.stringify(r.error));
    if (session.user?.kind === "device") await session.linkDeviceAccount();
    else await session.refresh();
    setMsg("signed up");
  }
  async function signIn() {
    const r = await authClient.signIn.email({ email, password });
    if (r.error) return setMsg("sign-in error: " + JSON.stringify(r.error));
    await session.refresh();
    setMsg("signed in");
  }
  return (
    <div>
      <input
        data-testid="email"
        placeholder="email"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
      />
      <input
        data-testid="password"
        placeholder="password"
        type="password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
      />
      <button data-testid="sign-up" onClick={() => void signUp()}>
        Sign up
      </button>
      <button data-testid="sign-in" onClick={() => void signIn()}>
        Sign in
      </button>
      <span data-testid="auth-msg">{msg}</span>
    </div>
  );
}
