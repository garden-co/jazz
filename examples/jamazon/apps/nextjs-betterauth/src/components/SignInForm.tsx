"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { FormLayout } from "@astryxdesign/core/FormLayout";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
import { VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { useRouter, useSearchParams } from "next/navigation";
import { useState } from "react";
import { Page } from "./Page";
import { useShopper } from "./StoreProviders";

export function SignInForm() {
  const shopper = useShopper();
  const router = useRouter();
  const next = useSearchParams().get("next");
  const [mode, setMode] = useState<"sign-in" | "sign-up">("sign-in");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);

  if (shopper.isSignedIn)
    return (
      <Page maxWidth={480}>
        <Card padding={6}>
          <VStack gap={4}>
            <Heading level={1}>Signed in</Heading>
            <Text>
              You're signed in as {shopper.name ?? shopper.email}. Your cart and orders sync to
              every device you sign in on.
            </Text>
            <Button label="Continue shopping" onClick={() => router.push(safeNext(next))} />
            <Button label="Sign out" variant="secondary" onClick={() => void shopper.signOut()} />
          </VStack>
        </Card>
      </Page>
    );

  async function submit() {
    setBusy(true);
    setError(undefined);
    try {
      if (mode === "sign-up") await shopper.signUp(name.trim() || email, email, password);
      else await shopper.signIn(email, password);
      router.push(safeNext(next));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
      setBusy(false);
    }
  }

  return (
    <Page maxWidth={480}>
      <Card padding={6}>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            void submit();
          }}
        >
          <FormLayout>
            <Heading level={1}>{mode === "sign-in" ? "Sign in" : "Create an account"}</Heading>
            <Text color="secondary">
              {mode === "sign-in"
                ? "Anything in your cart now is added to your account's cart."
                : "Your current cart becomes your account's cart."}
            </Text>
            <SegmentedControl
              label="Account"
              value={mode}
              onChange={(value) => setMode(value as typeof mode)}
            >
              <SegmentedControlItem value="sign-in" label="Sign in" />
              <SegmentedControlItem value="sign-up" label="Create account" />
            </SegmentedControl>
            {mode === "sign-up" && (
              <TextInput label="Name" value={name} onChange={setName} autoComplete="name" />
            )}
            <TextInput
              label="Email"
              type="email"
              value={email}
              onChange={setEmail}
              autoComplete="email"
              isRequired
            />
            <TextInput
              label="Password"
              type="password"
              value={password}
              onChange={setPassword}
              autoComplete={mode === "sign-in" ? "current-password" : "new-password"}
              description={mode === "sign-up" ? "At least 8 characters." : undefined}
              isRequired
            />
            {error && <Banner status="error" title="That didn't work" description={error} />}
            <Button
              type="submit"
              label={mode === "sign-in" ? "Sign in" : "Create account"}
              size="lg"
              isLoading={busy}
            />
          </FormLayout>
        </form>
      </Card>
    </Page>
  );
}

/** Only follow same-site paths. */
function safeNext(next: string | null): string {
  return next && next.startsWith("/") && !next.startsWith("//") ? next : "/";
}
