"use client";

import { useState, type FormEvent } from "react";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { FormLayout } from "@astryxdesign/core/FormLayout";
import { VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { authClient } from "@/src/lib/auth-client";

export function SignInForm() {
  const [signUp, setSignUp] = useState(false);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function submit(event: FormEvent) {
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
    window.location.assign("/chat");
  }

  return (
    <Card maxWidth={420} width="100%" padding={6}>
      <form onSubmit={submit}>
        <VStack gap={5}>
          <VStack gap={2}>
            <Heading level={1}>
              {signUp ? "Create your booking desk" : "Sign in to MusicAgent"}
            </Heading>
            <Text color="secondary">
              An assistant for a band's booking agent. It finds venues, checks the tour calendar and
              drafts setlists, and every reply streams to all your open devices.
            </Text>
          </VStack>
          <FormLayout>
            {signUp && <TextInput label="Name" value={name} onChange={setName} isRequired />}
            <TextInput label="Email" type="email" value={email} onChange={setEmail} isRequired />
            <TextInput
              label="Password"
              type="password"
              value={password}
              onChange={setPassword}
              description={signUp ? "At least 8 characters" : undefined}
              isRequired
            />
          </FormLayout>
          {error && <Banner status="error" title={error} />}
          <VStack gap={2}>
            <Button
              type="submit"
              variant="primary"
              label={signUp ? "Create account" : "Sign in"}
              isLoading={pending}
              width="100%"
            />
            <Button
              variant="ghost"
              label={signUp ? "I already have an account" : "Create an account"}
              onClick={() => setSignUp(!signUp)}
              width="100%"
            />
          </VStack>
        </VStack>
      </form>
    </Card>
  );
}
