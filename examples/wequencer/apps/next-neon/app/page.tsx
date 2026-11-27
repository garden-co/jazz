import { redirect } from "next/navigation";
import { Center } from "@astryxdesign/core/Center";
import { Heading } from "@astryxdesign/core/Heading";
import { Text } from "@astryxdesign/core/Text";
import { VStack } from "@astryxdesign/core/VStack";
import { getUser } from "@/lib/auth";
import { SignInForm } from "@/components/sign-in-form";

export default async function HomePage() {
  const user = await getUser();
  if (user) redirect("/dashboard");

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
