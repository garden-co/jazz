"use client";

import { useAuth } from "jazz-react-auth-betterauth";
import { redirect } from "next/navigation";

export default function Page() {
  const { signIn } = useAuth();
  signIn().then(redirect("/"));
  return null;
}
