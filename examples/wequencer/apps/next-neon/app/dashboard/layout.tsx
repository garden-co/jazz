import { redirect } from "next/navigation";
import { getUser } from "@/lib/auth";
import { AppFrame } from "@/components/app-frame";

export default async function DashboardLayout({ children }: { children: React.ReactNode }) {
  const user = await getUser();
  if (!user) redirect("/");
  return <AppFrame>{children}</AppFrame>;
}
