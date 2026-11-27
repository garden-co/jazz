"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { db } from "@/lib/db";
import { AppFrame } from "@/components/app-frame";
import { LoadingScreen } from "@/components/status-screen";

// Instant keeps the session in the browser, so the redirect happens here.
export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const { isLoading, user } = db.useAuth();
  useEffect(() => {
    if (!isLoading && !user) router.replace("/");
  }, [isLoading, router, user]);
  if (!user) return <LoadingScreen label="Opening Wequencer…" />;
  return <AppFrame>{children}</AppFrame>;
}
