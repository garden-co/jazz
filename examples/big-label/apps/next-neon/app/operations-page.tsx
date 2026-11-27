"use client";

import * as React from "react";
import {
  Banner,
  Button,
  Card,
  Center,
  Heading,
  Text,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { Operations } from "../src/App";
import { authClient } from "../src/lib/auth-client";

// Managed Better Auth's session cookie (on the app's origin) drives sign-in:
// app/page.tsx reads it on the server, and so do the sealing route, the write
// Server Action and the API routes. Signing in or out reloads the page.

function toError(cause: unknown) {
  return cause instanceof Error ? cause : new Error(String(cause));
}

function SignIn() {
  const [mode, setMode] = React.useState<"sign-in" | "sign-up">("sign-in");
  const [name, setName] = React.useState("");
  const [email, setEmail] = React.useState("label@example.com");
  const [password, setPassword] = React.useState("big-label-demo");
  const [error, setError] = React.useState<string | null>(null);
  const [pending, setPending] = React.useState(false);
  async function authenticate() {
    setPending(true);
    setError(null);
    const result =
      mode === "sign-in"
        ? await authClient.signIn.email({ email, password })
        : await authClient.signUp.email({ email, password, name: name.trim() || email });
    if (result.error) {
      setPending(false);
      setError(result.error.message ?? "Authentication failed");
    }
    // The server renders the signed-in app from the new session cookie.
    else window.location.reload();
  }
  return (
    <AuthScreen>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          void authenticate();
        }}
      >
        <VStack gap={4}>
          <VStack gap={1}>
            <Heading level={1}>
              {mode === "sign-in" ? "Sign in to BigLabel" : "Create an account"}
            </Heading>
            <Text color="secondary">
              Run artists, releases and teams for your labels, live on every device.
            </Text>
          </VStack>
          {mode === "sign-up" && (
            <TextInput label="Your name" value={name} onChange={setName} autoComplete="name" />
          )}
          <TextInput
            label="Email"
            type="email"
            value={email}
            onChange={setEmail}
            autoComplete="email"
          />
          <TextInput
            label="Password"
            type="password"
            value={password}
            onChange={setPassword}
            autoComplete={mode === "sign-in" ? "current-password" : "new-password"}
          />
          {error && <Banner status="error" title={error} />}
          <Button
            type="submit"
            label={mode === "sign-in" ? "Sign in" : "Create account"}
            isLoading={pending}
            width="100%"
          />
          <Button
            label={mode === "sign-in" ? "Create an account instead" : "I already have an account"}
            variant="ghost"
            onClick={() => {
              setMode(mode === "sign-in" ? "sign-up" : "sign-in");
              setError(null);
            }}
          />
        </VStack>
      </form>
    </AuthScreen>
  );
}

/** Centred card for the signed-out and connecting states. */
function AuthScreen({ children }: { children: React.ReactNode }) {
  return (
    <Center minHeight="100dvh" padding={4}>
      <Card padding={6} width="100%" maxWidth={420}>
        {children}
      </Card>
    </Center>
  );
}

export default function OperationsPage({ user }: { user: { id: string; email: string } | null }) {
  if (!user) return <SignIn />;
  return <AccountApp account={user.id} email={user.email} />;
}

function AccountApp({ account, email }: { account: string; email: string }) {
  const [bootstrapping, setBootstrapping] = React.useState(false);
  // A failed background bootstrap is shown as a banner over the working app.
  const [bootstrapError, setBootstrapError] = React.useState<Error>();
  const [attempt, setAttempt] = React.useState(0);

  React.useEffect(() => {
    // The personal label is created by the server once per account (and again
    // only when the sign-in email changes). The app renders meanwhile.
    if (!account || !email || readBootstrapped(account) === email) return;
    let current = true;
    setBootstrapError(undefined);
    setBootstrapping(true);
    bootstrapOnce(account, email)
      .catch((cause: unknown) => {
        if (current) setBootstrapError(toError(cause));
      })
      .finally(() => {
        if (current) setBootstrapping(false);
      });
    return () => {
      current = false;
    };
  }, [account, email, attempt]);

  return (
    <Operations
      account={account}
      email={email}
      preparing={bootstrapping}
      setupError={
        bootstrapError
          ? { message: bootstrapError.message, retry: () => setAttempt((value) => value + 1) }
          : null
      }
      // Signs out of Managed Better Auth; the reload drops the live queries.
      onSignOut={async () => {
        await authClient.signOut();
        window.location.reload();
      }}
    />
  );
}

const bootstrappedKey = (accountId: string) => `big-label-bootstrapped:${accountId}`;
const bootstrapRequests = new Map<string, Promise<void>>();

/** The sign-in email the account was last bootstrapped with, if any. */
function readBootstrapped(accountId: string): string | null {
  try {
    return localStorage.getItem(bootstrappedKey(accountId));
  } catch {
    return null;
  }
}

/** One bootstrap request per account and email at a time (StrictMode, Retry). */
function bootstrapOnce(accountId: string, email: string): Promise<void> {
  const key = `${accountId}:${email}`;
  let request = bootstrapRequests.get(key);
  if (!request) {
    request = (async () => {
      // The session cookie is the only credential the server needs.
      const response = await fetch("/api/bootstrap", {
        method: "POST",
        credentials: "same-origin",
      });
      if (!response.ok) throw new Error(`bootstrap failed (${response.status})`);
      try {
        localStorage.setItem(bootstrappedKey(accountId), email);
      } catch {
        // Storage unavailable: the next load asks the (idempotent) server again.
      }
    })().finally(() => bootstrapRequests.delete(key));
    bootstrapRequests.set(key, request);
  }
  return request;
}
