"use client";

import { useAccount } from "jazz-react";
import { redirect } from "next/navigation";

export default function Page() {
  const { logOut } = useAccount({ resolve: { profile: true } });
  logOut();
  redirect("/");
}
