"use client";

import { useAuth } from "jazz-react-auth-betterauth";
import { redirect } from "next/navigation";

export default function Page() {
  const { auth, user, account } = useAuth();
  auth.logIn().then(redirect("/"));
  return null;
}
