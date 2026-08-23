"use client";

import { useState } from "react";

/** Small auth surface; the Jazz application remains the point of the example. */
export default function Page() {
  const [message, setMessage] = useState("Sign in with Better Auth, then provision your label.");
  async function bootstrap() {
    const response = await fetch("/api/bootstrap", { method: "POST" });
    setMessage(response.ok ? "Your personal label is ready." : "Sign in before provisioning.");
  }
  return (
    <main>
      <h1>BigLabel</h1>
      <p>{message}</p>
      <button onClick={bootstrap}>Provision label</button>
    </main>
  );
}
