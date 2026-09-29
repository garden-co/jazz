import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { SignInForm } from "@/components/sign-in-form";
import { auth } from "@/src/lib/auth";

export default function Home({ searchParams }: { searchParams: Promise<{ join?: string }> }) {
  return <HomeContent searchParams={searchParams} />;
}

async function HomeContent({ searchParams }: { searchParams: Promise<{ join?: string }> }) {
  const session = await auth.api.getSession({ headers: await headers() });
  const { join } = await searchParams;
  if (session) redirect(join ? `/dashboard?join=${encodeURIComponent(join)}` : "/dashboard");
  return (
    <main className="centered-page">
      <SignInForm />
    </main>
  );
}
