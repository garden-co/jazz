"use client";

import { useState, type FormEvent } from "react";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { Heading } from "@astryxdesign/core/Heading";
import { HStack } from "@astryxdesign/core/HStack";
import { Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { VStack } from "@astryxdesign/core/VStack";
import { authClient } from "@/lib/auth-client";
import { beginSignupIntent, clearSignupIntent } from "@/components/jazz-provider";

export function SignInForm() {
  const [isSignUp, setIsSignUp] = useState(false);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, setIsPending] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    setIsPending(true);
    if (isSignUp) beginSignupIntent(email);
    const { error } = await (isSignUp
      ? authClient.signUp.email({ name, email, password })
      : authClient.signIn.email({ email, password }));
    if (error) {
      if (isSignUp) clearSignupIntent();
      setError(error.message ?? (isSignUp ? "Sign-up failed" : "Sign-in failed"));
      setIsPending(false);
      return;
    }
    window.location.assign("/dashboard");
  }

  return (
    <Card width="100%" maxWidth={400}>
      <form onSubmit={submit}>
        <VStack gap={4}>
          <Heading level={2}>{isSignUp ? "Create account" : "Sign in"}</Heading>
          {isSignUp && (
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
            autoComplete={isSignUp ? "new-password" : "current-password"}
          />
          {error && (
            <div role="alert">
              <Banner status="error" title={error} />
            </div>
          )}
          <Button
            type="submit"
            variant="primary"
            label={isSignUp ? "Create account" : "Sign in"}
            isLoading={isPending}
            width="100%"
          />
          <HStack gap={1} align="center" justify="center" wrap="wrap">
            <Text type="supporting">{isSignUp ? "Already have an account?" : "New here?"}</Text>
            <Button
              variant="ghost"
              size="sm"
              label={isSignUp ? "Sign in" : "Create an account"}
              onClick={() => {
                setError(null);
                setIsSignUp(!isSignUp);
              }}
            />
          </HStack>
        </VStack>
      </form>
    </Card>
  );
}
