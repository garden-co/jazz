import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { CenteredPage } from "@/components/centered-page";
import { SignInForm } from "@/components/sign-in-form";
import { auth } from "@/src/lib/auth";

export default function Home() {
  return <HomeContent />;
}

async function HomeContent() {
  const session = await auth.api.getSession({ headers: await headers() });
  // Browsers keep the URL fragment across this redirect, so an invite link
  // (`#invite/...`) survives without ever being sent to the server.
  if (session) redirect("/dashboard");
  return (
    <CenteredPage>
      <SignInForm />
    </CenteredPage>
  );
}
