import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { auth } from "@/lib/auth";
import { ensureProfile } from "@/lib/bootstrap";
import { JazzProvider } from "@/components/jazz-provider";

export default async function DashboardLayout({ children }: { children: React.ReactNode }) {
  const session = await auth.api.getSession({ headers: await headers() });
  if (!session) redirect("/");
  await ensureProfile(
    process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000",
    session.user.id,
    session.user.name,
  );
  return <JazzProvider>{children}</JazzProvider>;
}
