"use client";

import { useState } from "react";
import { JazzProvider, betterAuth } from "jazz-tools/react";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { Heading } from "@astryxdesign/core/Heading";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { authClient } from "../src/lib/auth-client";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
// Better Auth's session drives sign-in; RecordPlayer's backend (`createJazz`
// in src/lib/jazz.server.ts) reads it in `authenticate`. Signing up creates
// the listener's Jazz account, signing in opens it again.
const auth = betterAuth(authClient);

/**
 * `frame` wraps every state in the page chrome; only the signed-in one gets
 * a sign-out button, which needs Jazz.
 */
export function RecordPlayerProvider({
  children,
  frame,
}: {
  children: React.ReactNode;
  frame: (content: React.ReactNode, signedIn: boolean) => React.ReactNode;
}) {
  return (
    <JazzProvider
      appId={appId}
      serverUrl="/jazz"
      auth={auth}
      signedOut={frame(<SignIn />, false)}
      loading={frame(<Status>Connecting RecordPlayer…</Status>, false)}
      error={(state) =>
        frame(
          <Banner
            status="error"
            title="Could not connect RecordPlayer"
            description={state.error?.message}
            endContent={
              <Button
                label="Retry"
                variant="secondary"
                size="sm"
                onClick={() => void state.retry()}
              />
            }
          />,
          false,
        )
      }
    >
      {frame(children, true)}
    </JazzProvider>
  );
}

export function SignIn() {
  const [email, setEmail] = useState("listener@example.com");
  const [password, setPassword] = useState("record-player-demo");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  async function authenticate(mode: "sign-in" | "sign-up") {
    setPending(true);
    setError(null);
    try {
      const result =
        mode === "sign-in"
          ? await authClient.signIn.email({ email, password })
          : await authClient.signUp.email({ email, password, name: email });
      if (result.error) setError(result.error.message ?? "Authentication failed");
    } catch (cause) {
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
