"use client";

import { useState, type FormEvent } from "react";
import { Banner, Button, Card, Heading, Text, TextInput, VStack } from "@astryxdesign/core";
import { authClient } from "@/src/lib/auth-client";

/** Email and password via Better Auth, then on to `next` (the workspace or an invite). */
export function SignInForm({ next = "/workspace" }: { next?: string }) {
  const [mode, setMode] = useState<"sign-in" | "sign-up">("sign-in");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const signingUp = mode === "sign-up";

  async function submit(event: FormEvent) {
    event.preventDefault();
    setPending(true);
    setError(null);
    const { error } = signingUp
      ? await authClient.signUp.email({ name, email, password })
      : await authClient.signIn.email({ email, password });
    if (error) {
      setError(error.message ?? "Could not sign in");
      setPending(false);
      return;
    }
    window.location.assign(next);
  }

  return (
    <Card maxWidth={420} width="100%">
      <form onSubmit={submit}>
        <VStack gap={4}>
          <VStack gap={1}>
            <Heading level={1}>{signingUp ? "Create your account" : "Sign in to BandBook"}</Heading>
            <Text type="supporting">
              Setlists, lyrics, tour notes and the band's to-do list, in one shared notebook.
            </Text>
          </VStack>
          {signingUp && (
            <TextInput
              label="Name"
              value={name}
              onChange={setName}
              isRequired
              autoComplete="name"
            />
          )}
          <TextInput
            label="Email"
            type="email"
            value={email}
            onChange={setEmail}
            isRequired
            autoComplete="email"
          />
          <TextInput
            label="Password"
            type="password"
            value={password}
            onChange={setPassword}
            isRequired
            description={signingUp ? "At least 8 characters" : undefined}
            autoComplete={signingUp ? "new-password" : "current-password"}
          />
          {error && <Banner status="error" title={error} collapsible={false} />}
          <VStack gap={2}>
            <Button
              type="submit"
              variant="primary"
              label={signingUp ? "Create account" : "Sign in"}
              isLoading={pending}
              width="100%"
            />
            <Button
              variant="ghost"
              label={signingUp ? "I already have an account" : "Create an account"}
              onClick={() => setMode(signingUp ? "sign-in" : "sign-up")}
              width="100%"
            />
          </VStack>
        </VStack>
      </form>
    </Card>
  );
}
