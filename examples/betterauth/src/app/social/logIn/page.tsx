"use client";

import { useAuth } from "@/contexts/Auth";
import { redirect } from "next/navigation";

export default function Page() {
  const auth = useAuth();
  auth.logIn().then(redirect("/"));
  return null;
}
