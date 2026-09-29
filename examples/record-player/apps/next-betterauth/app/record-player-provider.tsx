"use client";

import { createContext, useContext, useEffect, useMemo, useRef, useState } from "react";
import { createJazzClient, JazzClientProvider, type JazzClient } from "jazz-tools/react";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { Heading } from "@astryxdesign/core/Heading";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { authClient, getJwtFromBetterAuth } from "../src/lib/auth-client";
import { prepareAccounts } from "../src/lib/accounts";
import { JazzLifecycle } from "../src/lib/jazz-lifecycle";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;
const registerIntentKey = "record-player-register-jwt";
type Accounts = Awaited<ReturnType<typeof prepareAccounts>>;
const LifecycleContext = createContext<JazzLifecycle | null>(null);

export function useRecordPlayerLifecycle(): JazzLifecycle {
  const lifecycle = useContext(LifecycleContext);
  if (!lifecycle) throw new Error("RecordPlayer Jazz lifecycle is not ready.");
  return lifecycle;
}

export function RecordPlayerProvider({ children }: { children: React.ReactNode }) {
  const { data: session, isPending } = authClient.useSession();
  const [accounts, setAccounts] = useState<Accounts | null>(null);
  const [startupError, setStartupError] = useState<Error>();
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    let cancelled = false;
    void prepareAccounts(appId, serverUrl).then(
      (prepared) => !cancelled && setAccounts(prepared),
      (cause) =>
        !cancelled && setStartupError(cause instanceof Error ? cause : new Error(String(cause))),
    );
    return () => {
      cancelled = true;
    };
  }, [attempt]);
  if (startupError)
    return (
      <Banner
        status="error"
        title="Could not start RecordPlayer"
        description={startupError.message}
        endContent={
          <Button
            label="Retry"
            variant="secondary"
            size="sm"
            onClick={() => {
              setStartupError(undefined);
              setAttempt((value) => value + 1);
            }}
          />
        }
      />
    );
  if (isPending) return <Status>Preparing your RecordPlayer…</Status>;
  if (!session?.user) return <SignIn />;
  if (!accounts) return <Status>Connecting RecordPlayer…</Status>;
  return (
    <AccountContext accounts={accounts} principal={session.user.id} sessionId={session.session.id}>
      {children}
    </AccountContext>
  );
}

function AccountContext({
  accounts,
  principal,
  sessionId,
  children,
}: {
  accounts: Accounts;
  principal: string;
  sessionId: string;
  children: React.ReactNode;
}) {
  const [client, setClient] = useState<JazzClient>();
  const [error, setError] = useState<Error>();
  const lifecycleRef = useRef<JazzLifecycle | undefined>(undefined);
  if (!lifecycleRef.current) {
    lifecycleRef.current = new JazzLifecycle(
      accounts,
      (account) => createJazzClient({ appId, env: "dev", serverUrl, account }),
      setClient,
    );
  }
  const lifecycle = lifecycleRef.current;
  const registering = sessionStorage.getItem(registerIntentKey) === "1";
  const enroll = useMemo(
    () =>
      registering
        ? (manager: Accounts) => manager.registerJWT({ getToken: requireBetterAuthToken })
        : (manager: Accounts) => manager.loginJWT({ getToken: requireBetterAuthToken }),
    [registering],
  );

  useEffect(() => {
    let active = true;
    void lifecycle.reconcile(principal, sessionId, enroll).then(
      () => {
        if (active) {
          setError(undefined);
          sessionStorage.removeItem(registerIntentKey);
        }
      },
      (cause) => active && setError(cause instanceof Error ? cause : new Error(String(cause))),
    );
    return () => {
      active = false;
    };
  }, [enroll, lifecycle, principal, sessionId]);
  useEffect(
    () => () => {
      void lifecycle
        .close()
        .catch((cause) => console.error("RecordPlayer Jazz shutdown failed", cause));
    },
    [lifecycle],
  );

  const visibleClient = client && lifecycle.isCurrent(principal, sessionId) ? client : undefined;
  if (error && !visibleClient)
    return (
      <Banner
        status="error"
        title="Could not connect RecordPlayer"
        description={error.message}
        endContent={
          <Button
            label="Retry"
            variant="secondary"
            size="sm"
            onClick={() =>
              void lifecycle.reconcile(principal, sessionId, enroll).then(
                () => setError(undefined),
                (cause) => setError(cause instanceof Error ? cause : new Error(String(cause))),
              )
            }
          />
        }
      />
    );
  if (!visibleClient) return <Status>Connecting RecordPlayer…</Status>;
  return (
    <LifecycleContext.Provider value={lifecycle}>
      {error && (
        <Banner status="error" title="Could not update RecordPlayer" description={error.message} />
      )}
      <JazzClientProvider client={visibleClient}>{children}</JazzClientProvider>
    </LifecycleContext.Provider>
  );
}

async function requireBetterAuthToken(): Promise<string> {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Better Auth did not provide a Jazz session token.");
  return token;
}

function SignIn() {
  const [email, setEmail] = useState("listener@example.com");
  const [password, setPassword] = useState("record-player-demo");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  async function authenticate(mode: "sign-in" | "sign-up") {
    sessionStorage.setItem(registerIntentKey, mode === "sign-up" ? "1" : "0");
    setPending(true);
    setError(null);
    try {
      const result =
        mode === "sign-in"
          ? await authClient.signIn.email({ email, password })
          : await authClient.signUp.email({ email, password, name: email });
      if (result.error) {
        sessionStorage.removeItem(registerIntentKey);
        setError(result.error.message ?? "Authentication failed");
      }
    } catch (cause) {
      sessionStorage.removeItem(registerIntentKey);
      setError(cause instanceof Error ? cause.message : "Authentication failed");
    } finally {
      setPending(false);
    }
  }
  return (
    <div className="rp-sign-in">
      <Card padding={6}>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            void authenticate("sign-in");
          }}
        >
          <VStack gap={4}>
            <VStack gap={1}>
              <Heading level={1}>Sign in to RecordPlayer</Heading>
              <Text color="secondary">
                A shared music library with playlists you can share. The demo account is filled in;
                use it or create your own.
              </Text>
            </VStack>
            <TextInput label="Email" type="email" value={email} onChange={setEmail} />
            <TextInput label="Password" type="password" value={password} onChange={setPassword} />
            <HStack gap={2} wrap="wrap">
              <Button label="Sign in" type="submit" variant="primary" isDisabled={pending} />
              <Button
                label="Create account"
                variant="secondary"
                isDisabled={pending}
                onClick={() => void authenticate("sign-up")}
              />
            </HStack>
            {error && <Banner status="error" title="Could not sign in" description={error} />}
          </VStack>
        </form>
      </Card>
    </div>
  );
}

function Status({ children }: { children: React.ReactNode }) {
  return (
    <Text color="secondary" role="status">
      {children}
    </Text>
  );
}
