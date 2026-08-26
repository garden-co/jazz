import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { JazzProvider } from "@/components/jazz-provider";
import { auth } from "@/src/lib/auth";
import { ensureProfile } from "@/src/lib/bootstrap";

export default async function DashboardLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  const session = await auth.api.getSession({ headers: await headers() });
  if (!session) redirect("/");
  await ensureProfile(session.user.id, session.user.name);
  return <JazzProvider>{children}</JazzProvider>;
}
