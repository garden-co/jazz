import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { SignInForm } from "@/components/sign-in-form";
import { auth } from "@/src/lib/auth";
export default function Home() {
  return <HomeContent />;
}

async function HomeContent() {
  const session = await auth.api.getSession({ headers: await headers() });
  if (session) redirect("/dashboard");
  return (
    <main>
      <p>LOCAL-FIRST POSTER STUDIO</p>
      <SignInForm />
    </main>
  );
}
