import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { SignInForm } from "@/components/sign-in-form";
import { auth } from "@/src/lib/auth";

export default async function HomePage() {
  const session = await auth.api.getSession({ headers: await headers() });
  if (session) redirect("/dashboard");
  return (
    <main className="shell">
      <header>
        <span className="eyebrow">LOCAL-FIRST BAND HQ</span>
        <h1>BandChat</h1>
        <p>Better Auth guards the app before Jazz mounts the ordinary room and message UI.</p>
      </header>
      <SignInForm />
    </main>
  );
}
