import { useActionState, useState } from "react";
import { Banner, Button, Card, Center, Heading, Text, TextInput, VStack } from "@astryxdesign/core";
import { useAuthActions } from "@convex-dev/auth/react";

type SignIn = ReturnType<typeof useAuthActions>["signIn"];

async function authenticate(signIn: SignIn, formData: FormData): Promise<string | null> {
  const name = formData.get("name");
  const registering = !!name;
  formData.set("flow", registering ? "signUp" : "signIn");
  try {
    await signIn("password", formData);
  } catch {
    // Convex Auth's server errors aren't meant for people; say what happened.
    return registering ? "Could not create the account" : "Invalid email or password";
  }
  return null;
}

export function SignInForm() {
  const [signingUp, setSigningUp] = useState(false);
  const { signIn } = useAuthActions();
  const [error, formAction, pending] = useActionState(
    (_previous: string | null, formData: FormData) => authenticate(signIn, formData),
    null,
  );
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
