import { Button, Card, Spinner, TextField } from "@radix-ui/themes";
import { useState, type FormEvent } from "react";
import { SuccessIcon } from "./Icons.js";

export function LoginView({ sessionError }: { sessionError?: string }) {
  const [handle, setHandle] = useState("");
  const [loggingIn, setLoggingIn] = useState(false);
  const [loginError, setLoginError] = useState<string | null>(null);

  async function beginLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoggingIn(true);
    setLoginError(null);
    try {
      const response = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ handle }),
      });
      const result = (await response.json()) as { url?: string; error?: string };
      if (!response.ok || !result.url) throw new Error(result.error ?? "Could not start sign-in");
      location.href = result.url;
    } catch (error) {
      setLoggingIn(false);
      setLoginError(error instanceof Error ? error.message : "Could not start sign-in");
    }
  }

  const status = loginError ?? sessionError ?? (loggingIn ? "Opening secure sign-in…" : undefined);

  return (
    <main className="login">
      <section className="login-intro">
        <div className="brand-lockup">
          <div className="brand-mark" aria-hidden="true">
            J
          </div>
          <div>
            <p className="eyebrow">Local-first ATProto</p>
            <p className="wordmark">Jazz ❤️ Bluesky</p>
          </div>
        </div>
        <h1>Keep up, even when your connection can’t.</h1>
        <p>
          Jazz and Bluesky together: a familiar timeline that loads offline and lets you write
          whenever inspiration strikes.
        </p>
        <ul>
          <li>
            <span>
              <SuccessIcon />
            </span>
            <div>
              <strong>Read anywhere</strong>
              <p>Your recent timeline is cached on this device.</p>
            </div>
          </li>
          <li>
            <span>
              <SuccessIcon />
            </span>
            <div>
              <strong>Write offline</strong>
              <p>Posts and reactions queue locally and publish when you reconnect.</p>
            </div>
          </li>
          <li>
            <span>
              <SuccessIcon />
            </span>
            <div>
              <strong>Stay in control</strong>
              <p>Your PDS remains the source of truth.</p>
            </div>
          </li>
        </ul>
      </section>
      <Card asChild size="4">
        <section className="login-card" aria-labelledby="login-title">
          <p className="eyebrow">Welcome</p>
          <h2 id="login-title">Connect your Bluesky account</h2>
          <p>Enter your handle to continue through ATProto OAuth at your PDS.</p>
          <form onSubmit={beginLogin} aria-busy={loggingIn}>
            <label htmlFor="handle">Bluesky handle</label>
            <TextField.Root
              id="handle"
              size="3"
              value={handle}
              onChange={(event) => setHandle(event.target.value)}
              placeholder="you.bsky.social"
              autoCapitalize="none"
              autoCorrect="off"
              required
            />
            <Button type="submit" size="3" disabled={loggingIn || !handle.trim()}>
              {loggingIn ? (
                <>
                  <Spinner aria-hidden="true" />
                  Opening ATProto OAuth…
                </>
              ) : (
                "Continue with ATProto OAuth"
              )}
            </Button>
            {status && (
              <p
                className={loginError || sessionError ? "form-status error" : "form-status"}
                role="status"
                aria-live="polite"
              >
                {status}
              </p>
            )}
          </form>
          <p className="oauth-note">Jazz ❤️ Bluesky never asks for your Bluesky password.</p>
        </section>
      </Card>
    </main>
  );
}
