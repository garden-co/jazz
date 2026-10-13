"use client";

import { Banner, Button, Card, Heading, Text, TextInput, VStack } from "@astryxdesign/core";
import { useState, type FormEvent } from "react";
import { authClient } from "@/src/lib/auth-client";

export function SignInForm() {
  const [signUp, setSignUp] = useState(false);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setPending(true);
    setError(null);
    const { error } = await (signUp
      ? authClient.signUp.email({ name, email, password })
      : authClient.signIn.email({ email, password }));
    if (error) {
      setError(error.message ?? "Authentication failed");
      setPending(false);
      return;
    }
    window.location.assign(`/dashboard${window.location.hash}`);
  };

  return (
    <Card padding={6} width="100%" maxWidth="26rem">
      <form onSubmit={submit}>
        <VStack gap={4}>
          <VStack gap={1}>
            <Heading level={1}>PosterShop</Heading>
            <Text color="secondary">
              Design gig posters together. Every change syncs live and works offline.
            </Text>
          </VStack>
          <Heading level={2}>{signUp ? "Create an account" : "Sign in"}</Heading>
          {signUp && (
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
            autoComplete={signUp ? "new-password" : "current-password"}
            description={signUp ? "At least 8 characters" : undefined}
          />
          {error && <Banner status="error" title={error} />}
          <Button
            type="submit"
            label={signUp ? "Create account" : "Sign in"}
            variant="primary"
            isLoading={pending}
            width="100%"
          />
          <Button
            label={signUp ? "I already have an account" : "Create an account"}
            variant="ghost"
            width="100%"
            onClick={() => {
              setSignUp(!signUp);
              setError(null);
            }}
          />
        </VStack>
      </form>
    </Card>
  );
}
