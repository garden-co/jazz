"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { Center } from "@astryxdesign/core/Center";
import { VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { useState, type FormEvent } from "react";
import { authClient } from "@/src/lib/auth-client";

export function SignIn() {
  const [mode, setMode] = useState<"sign-in" | "sign-up">("sign-in");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string>();
  const [pending, setPending] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setPending(true);
    setError(undefined);
    const { error } = await (mode === "sign-up"
      ? authClient.signUp.email({ name, email, password })
      : authClient.signIn.email({ email, password }));
    setPending(false);
    if (error) setError(error.message ?? "Authentication failed");
  }

  return (
    <Center minHeight="100dvh" padding={4}>
      <Card padding={6} width="100%" maxWidth={420}>
        <form onSubmit={submit}>
          <VStack gap={4}>
            <VStack gap={1}>
              <Heading level={1}>
                {mode === "sign-up" ? "Create an operator account" : "Sign in"}
              </Heading>
              <Text color="secondary">
                Jamazon Warehouse operators enter orders, deliver batches and keep stock above its
                reorder level.
              </Text>
            </VStack>
            {mode === "sign-up" && (
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
              description={mode === "sign-up" ? "At least 8 characters." : undefined}
              autoComplete={mode === "sign-up" ? "new-password" : "current-password"}
            />
            {error && <Banner status="error" title={error} />}
            <Button
              type="submit"
              variant="primary"
              label={mode === "sign-up" ? "Create account" : "Sign in"}
              isLoading={pending}
              width="100%"
            />
            <Button
              variant="ghost"
              label={
                mode === "sign-up" ? "I already have an account" : "Create an operator account"
              }
              onClick={() => {
                setMode(mode === "sign-up" ? "sign-in" : "sign-up");
                setError(undefined);
              }}
              width="100%"
            />
          </VStack>
        </form>
      </Card>
    </Center>
  );
}
