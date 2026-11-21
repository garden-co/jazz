"use client";

import * as React from "react";
import { useConvexAuth, useMutation, useQuery } from "convex/react";
import { useAuthActions } from "@convex-dev/auth/react";
import {
  Banner,
  Button,
  Card,
  Center,
  HStack,
  Heading,
  Spinner,
  Text,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { Operations } from "../src/App";
import { api } from "../convex/_generated/api";
import { ConvexClientProvider } from "../src/lib/convex";

// Convex Auth (email and password) signs people in; every query and mutation
// then runs as that user.

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
  const { signIn } = useAuthActions();
  async function authenticate() {
    setPending(true);
    setError(null);
    try {
      await signIn("password", {
        flow: mode === "sign-in" ? "signIn" : "signUp",
        email,
        password,
        ...(mode === "sign-up" ? { name: name.trim() || email } : {}),
      });
    } catch {
      // Convex Auth's server errors aren't meant for people; say what happened.
      setError(mode === "sign-in" ? "Invalid email or password" : "Authentication failed");
    } finally {
      setPending(false);
    }
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

function Waiting({ message }: { message: string }) {
  return (
    <AuthScreen>
      <HStack gap={3} vAlign="center">
        <Spinner size="sm" />
        <Text>{message}</Text>
      </HStack>
    </AuthScreen>
  );
}

export default function Page() {
  return (
    <ConvexClientProvider>
      <Session />
    </ConvexClientProvider>
  );
}

function Session() {
  const { isLoading, isAuthenticated } = useConvexAuth();
  if (isLoading) return <Waiting message="Preparing your personal label…" />;
  return isAuthenticated ? <AccountApp /> : <SignIn />;
}

function AccountApp() {
  const viewer = useQuery(api.users.viewer);
  const account = viewer?.id;
  const email = viewer?.email ?? "";
  const bootstrap = useMutation(api.bootstrap.ensurePersonalOrganization);
  const { signOut } = useAuthActions();
  const [bootstrapping, setBootstrapping] = React.useState(false);
  // A failed background bootstrap is shown as a banner over the working app,
  // not the full-screen failure: the account and its local data are fine.
  const [bootstrapError, setBootstrapError] = React.useState<Error>();
  const [attempt, setAttempt] = React.useState(0);

  React.useEffect(() => {
    // The personal label is created by the server once per account (and again
    // only when the sign-in email changes). The app renders from local data
    // meanwhile.
    if (!account || !email || readBootstrapped(account) === email) return;
    let current = true;
    setBootstrapError(undefined);
    setBootstrapping(true);
    bootstrapOnce(account, email, bootstrap)
      .catch((cause: unknown) => {
        if (current) setBootstrapError(toError(cause));
      })
      .finally(() => {
        if (current) setBootstrapping(false);
      });
    return () => {
      current = false;
    };
  }, [account, email, attempt, bootstrap]);

  return (
    <Operations
      email={email}
      preparing={bootstrapping}
      setupError={
        bootstrapError
          ? { message: bootstrapError.message, retry: () => setAttempt((value) => value + 1) }
          : null
      }
      onSignOut={() => void signOut()}
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
function bootstrapOnce(
  accountId: string,
  email: string,
  bootstrap: () => Promise<unknown>,
): Promise<void> {
  const key = `${accountId}:${email}`;
  let request = bootstrapRequests.get(key);
  if (!request) {
    request = (async () => {
      // The server decides what to create, for the signed-in user only.
      await bootstrap();
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
