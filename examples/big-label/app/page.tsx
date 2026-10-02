"use client";

import * as React from "react";
import {
  createAccountManager,
  createJazzClient,
  JazzClientProvider,
  type AccountManager,
  type JazzClient,
  type JWTAuth,
} from "jazz-tools/react";
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
import { authClient, getJwtFromBetterAuth } from "../src/lib/auth-client";
import { beginSignupIntent, clearSignupIntent, enrollAndBootstrap } from "../src/lib/enrollment";
import { JazzLifecycle } from "../src/lib/jazz-lifecycle";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;

type ManagerSlot = {
  sessionId: string;
  identityId: string;
  manager?: AccountManager<JWTAuth>;
  error?: Error;
};

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
    if (mode === "sign-up") beginSignupIntent(sessionStorage, email);
    const result =
      mode === "sign-in"
        ? await authClient.signIn.email({ email, password })
        : await authClient.signUp.email({ email, password, name: name.trim() || email });
    setPending(false);
    if (result.error) {
      if (mode === "sign-up") clearSignupIntent(sessionStorage);
      setError(result.error.message ?? "Authentication failed");
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

function Failure({ error, onRetry }: { error: Error; onRetry: () => void }) {
  return (
    <AuthScreen>
      <VStack gap={4}>
        <Banner status="error" title="BigLabel couldn't connect" description={error.message} />
        <Button label="Retry" variant="secondary" onClick={onRetry} />
      </VStack>
    </AuthScreen>
  );
}

export default function Page() {
  const { data, isPending } = authClient.useSession();
  const [slot, setSlot] = React.useState<ManagerSlot | null>(null);
  const [retry, setRetry] = React.useState(0);
  const sessionId = data?.session.id;
  const identityId = data?.user.id;

  React.useEffect(() => {
    if (!sessionId || !identityId) {
      setSlot(null);
      return;
    }
    let cancelled = false;
    setSlot({ sessionId, identityId });
    void createAccountManager({ appId, serverUrl, env: "big-label" })
      .then((manager) => {
        if (!cancelled) setSlot({ sessionId, identityId, manager });
      })
      .catch((cause) => {
        if (!cancelled) setSlot({ sessionId, identityId, error: toError(cause) });
      });
    return () => {
      cancelled = true;
    };
  }, [identityId, retry, sessionId]);

  if (isPending) return <Waiting message="Loading your session…" />;
  if (!sessionId || !identityId || !data) return <SignIn />;
  // Never let session B render session A's manager or client while effects
  // prepare the replacement manager.
  if (slot?.sessionId !== sessionId || slot.identityId !== identityId)
    return <Waiting message="Preparing your personal label…" />;
  if (slot.error)
    return <Failure error={slot.error} onRetry={() => setRetry((value) => value + 1)} />;
  if (!slot.manager) return <Waiting message="Preparing your personal label…" />;
  return (
    <AccountApp
      key={sessionId}
      accounts={slot.manager}
      email={data.user.email}
      identityId={identityId}
    />
  );
}

function AccountApp({
  accounts,
  email,
  identityId,
}: {
  accounts: AccountManager<JWTAuth>;
  email: string;
  identityId: string;
}) {
  const [client, setClient] = React.useState<JazzClient>();
  const [ready, setReady] = React.useState(false);
  const [bootstrapping, setBootstrapping] = React.useState(false);
  const [error, setError] = React.useState<Error>();
  const [retry, setRetry] = React.useState(0);
  const active = React.useRef(true);
  const lifecycleRef = React.useRef<JazzLifecycle | undefined>(undefined);
  if (!lifecycleRef.current)
    lifecycleRef.current = new JazzLifecycle(
      accounts,
      (account) => createJazzClient({ appId, serverUrl, account }),
      setClient,
    );
  const lifecycle = lifecycleRef.current;

  React.useEffect(() => {
    // Each run owns its own flag: strict mode's discarded run must stay
    // discarded after the next run marks the component active again.
    let current = true;
    active.current = true;
    setError(undefined);
    setReady(false);
    enrollAndBootstrap({
      lifecycle,
      storage: sessionStorage,
      email,
      identityId,
      getToken: requireJazzToken,
      // The personal label is created by the server once per account (and
      // again only when the sign-in email changes). The app renders from local
      // data meanwhile.
      needsBootstrap: (accountId) => readBootstrapped(accountId) !== email,
      onBootstrapStart: () => setBootstrapping(true),
      bootstrap: async (token, accountId) => {
        try {
          await bootstrapOnce(accountId, token, email);
        } finally {
          if (current) setBootstrapping(false);
        }
      },
      onBootstrapError: (cause) => {
        if (current) setError(toError(cause));
      },
      isCurrent: () => current,
    }).then(
      (finished) => {
        if (finished) setReady(true);
      },
      (cause) => {
        if (current) setError(toError(cause));
      },
    );
    return () => {
      current = false;
      active.current = false;
      void lifecycle.close().catch((cause) => console.error("Jazz shutdown failed", cause));
    };
  }, [email, identityId, lifecycle, retry]);

  const signOut = React.useCallback(async () => {
    try {
      await lifecycle.transition(async (manager) => {
        // Keep the account selected until Better Auth succeeds. If it rejects,
        // the lifecycle reopens the selected client and this error stays here.
        await authClient.signOut();
        manager.logout();
      });
    } catch (cause) {
      const next = toError(cause);
      if (active.current) setError(next);
      throw next;
    }
  }, [lifecycle]);

  if (error) return <Failure error={error} onRetry={() => setRetry((value) => value + 1)} />;
  if (!client || !ready) return <Waiting message="Preparing your personal label…" />;
  return (
    <JazzClientProvider client={client}>
      <Operations
        email={email}
        preparing={bootstrapping}
        onSignOut={() => void signOut().catch(() => {})}
      />
    </JazzClientProvider>
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
function bootstrapOnce(accountId: string, token: string, email: string): Promise<void> {
  const key = `${accountId}:${email}`;
  let request = bootstrapRequests.get(key);
  if (!request) {
    request = (async () => {
      const response = await fetch("/api/bootstrap", {
        method: "POST",
        headers: { authorization: `Bearer ${token}` },
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

async function requireJazzToken() {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Better Auth did not issue a Jazz token.");
  return token;
}
