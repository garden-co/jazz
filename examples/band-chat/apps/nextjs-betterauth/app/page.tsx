import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { SignInForm } from "@/components/sign-in-form";
import { auth } from "@/src/lib/auth";

export default async function HomePage() {
  const session = await auth.api.getSession({ headers: await headers() });
  if (session) redirect("/dashboard");
  return (
    <main className="shell">
      <span className="eyebrow">LOCAL-FIRST BAND HQ</span>
      <h1>BandChat</h1>
      <p>Sign in to create private rooms, invite bandmates, and keep writing while offline.</p>
      <SignInForm />
    </main>
  );
}
