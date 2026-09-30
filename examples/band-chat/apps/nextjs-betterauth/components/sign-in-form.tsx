"use client";

import { useActionState, useState } from "react";
import { Banner, Button, Card, Center, Heading, Text, TextInput, VStack } from "@astryxdesign/core";
import { authClient } from "@/src/lib/auth-client";

async function authenticate(_previous: string | null, formData: FormData): Promise<string | null> {
  const email = String(formData.get("email") ?? "");
  const password = String(formData.get("password") ?? "");
  const name = formData.get("name");
  const registering = !!name;
  // Persist this before Better Auth publishes the new session. The dashboard
  // provider may render before this form is unmounted.
  window.sessionStorage.setItem("band-chat-register-jwt", registering ? "1" : "0");
  let result;
  try {
    result = registering
      ? await authClient.signUp.email({ name: String(name), email, password })
      : await authClient.signIn.email({ email, password });
  } catch (cause) {
    window.sessionStorage.removeItem("band-chat-register-jwt");
    return cause instanceof Error ? cause.message : "Authentication failed";
  }
  if (result.error) {
    window.sessionStorage.removeItem("band-chat-register-jwt");
    return result.error.message ?? "Authentication failed";
  }
  window.location.assign("/dashboard");
  return null;
}

export function SignInForm() {
  const [signingUp, setSigningUp] = useState(false);
  const [error, formAction, pending] = useActionState(authenticate, null);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  return (
    <Center axis="both" padding={4} className="page-fill">
      <VStack gap={6} width="100%" maxWidth={400}>
        <VStack gap={2}>
          <Heading level={1}>BandChat</Heading>
          <Text color="secondary">
            Private rooms for your band. Messages, files and sketches are saved on your device
            first, so you can keep writing offline.
          </Text>
        </VStack>
        <Card width="100%">
          <form action={formAction}>
            <VStack gap={4}>
              <Heading level={2}>{signingUp ? "Create an account" : "Sign in"}</Heading>
              {signingUp ? (
                <TextInput
                  label="Name"
                  htmlName="name"
                  isRequired
                  value={name}
                  onChange={setName}
                  autoComplete="name"
                />
              ) : null}
              <TextInput
                label="Email"
                type="email"
                htmlName="email"
                isRequired
                value={email}
                onChange={setEmail}
                autoComplete="email"
              />
              <TextInput
                label="Password"
                type="password"
                htmlName="password"
                isRequired
                value={password}
                onChange={setPassword}
                description={signingUp ? "At least 8 characters." : undefined}
                autoComplete={signingUp ? "new-password" : "current-password"}
              />
              {error ? <Banner status="error" title={error} container="section" /> : null}
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
                onClick={() => setSigningUp(!signingUp)}
                width="100%"
              />
            </VStack>
          </form>
        </Card>
      </VStack>
    </Center>
  );
}
