import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { Center } from "@astryxdesign/core/Center";
import { SignInForm } from "@/components/sign-in-form";
import { auth } from "@/src/lib/auth";

export default async function Home() {
  const session = await auth.api.getSession({ headers: await headers() });
  if (session) redirect("/chat");
  return (
    <Center minHeight="100vh" padding={4}>
      <SignInForm />
    </Center>
  );
}
