"use client";

import { JazzAccount } from "@/schema";
import { useAccount } from "jazz-tools/react";
import { useRouter } from "next/navigation";
import { useEffect } from "react";

export default function Home() {
  const me = useAccount(JazzAccount);
  const router = useRouter();

  useEffect(() => {
    if (me.$isLoaded) {
      router.push(`/account/${me.$jazz.id}`);
    }
  }, [me.$isLoaded, router]);

  return null;
}
