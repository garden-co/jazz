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
import { useAuthActions } from "@convex-dev/auth/react";

export function SignInForm() {
  const [isSignUp, setIsSignUp] = useState(false);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, setIsPending] = useState(false);
  const { signIn } = useAuthActions();

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    setIsPending(true);
    try {
      await signIn("password", {
        flow: isSignUp ? "signUp" : "signIn",
        email,
        password,
        ...(isSignUp ? { name } : {}),
      });
      window.location.assign("/dashboard");
    } catch {
      // Convex Auth's server errors aren't meant for people; say what happened.
      setError(isSignUp ? "Sign-up failed" : "Invalid email or password");
      setIsPending(false);
    }
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
