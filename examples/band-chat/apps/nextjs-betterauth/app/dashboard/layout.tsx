import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { JazzProvider } from "@/components/jazz-provider";
import { auth } from "@/src/lib/auth";

export default async function DashboardLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  const session = await auth.api.getSession({ headers: await headers() });
  if (!session) redirect("/");
  return <JazzProvider>{children}</JazzProvider>;
}
