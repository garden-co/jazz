"use client";

import { useAuth } from "@/contexts/Auth";
import { redirect } from "next/navigation";

export default function Page() {
  const { auth, user, account } = useAuth();
  auth.signIn().then(redirect("/"));
  return null;
}
