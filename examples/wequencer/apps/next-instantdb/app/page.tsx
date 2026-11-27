"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { Center } from "@astryxdesign/core/Center";
import { Heading } from "@astryxdesign/core/Heading";
import { Text } from "@astryxdesign/core/Text";
import { VStack } from "@astryxdesign/core/VStack";
import { db } from "@/lib/db";
import { SignInForm } from "@/components/sign-in-form";

// Instant keeps the session in the browser, so the redirect happens here,
// for a session that exists when the page loads (signing in navigates itself).
export default function HomePage() {
  const router = useRouter();
  const { isLoading, user } = db.useAuth();
  const [wasSignedIn] = useState(() => ({ checked: false }));
  useEffect(() => {
    if (isLoading || wasSignedIn.checked) return;
    wasSignedIn.checked = true;
    if (user) router.replace("/dashboard");
  }, [isLoading, router, user, wasSignedIn]);

  return (
    <Center minHeight="100dvh" padding={4}>
      <VStack gap={6} align="center" width="100%" maxWidth={400}>
        <VStack gap={2} align="center">
          <Heading level={1}>Wequencer</Heading>
          <Text color="secondary" justify="center">
            A step sequencer your band edits together, in real time and offline.
          </Text>
        </VStack>
        <SignInForm />
      </VStack>
    </Center>
  );
}
