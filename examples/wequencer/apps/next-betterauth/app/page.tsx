import { headers } from "next/headers";
import { redirect } from "next/navigation";
import Image from "next/image";
import { auth } from "@/lib/auth";
import { SignInForm } from "@/components/sign-in-form";

export default async function HomePage() {
  const session = await auth.api.getSession({ headers: await headers() });
  if (session) redirect("/dashboard");

  return (
    <main className="page-center">
      <Image
        src="/jazz.svg"
        alt="Jazz"
        className="wordmark"
        width={80}
        height={24}
        style={{ width: "100%", height: "auto" }}
        loading="eager"
      />
      <SignInForm />
    </main>
  );
}
