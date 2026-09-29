import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { VStack } from "@astryxdesign/core";
import { SignInForm } from "@/components/sign-in-form";
import { auth } from "@/src/lib/auth";

export default async function Home({ searchParams }: { searchParams: Promise<{ next?: string }> }) {
  const { next } = await searchParams;
  // Only same-app paths, so the sign-in page cannot be used as an open redirect.
  const destination = next?.startsWith("/") && !next.startsWith("//") ? next : "/workspace";
  const session = await auth.api.getSession({ headers: await headers() });
  if (session) redirect(destination);
  return (
    <VStack as="main" height="100dvh" justify="center" align="center" padding={4}>
      <SignInForm next={destination} />
    </VStack>
  );
}
