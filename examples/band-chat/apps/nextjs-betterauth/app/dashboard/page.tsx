"use client";

import { BandChat } from "@/src/BandChat";
import { authClient } from "@/src/lib/auth-client";

export default function DashboardPage() {
  async function signOut() {
    await authClient.signOut();
    window.location.assign("/");
  }
  return (
    <>
      <button className="sign-out" onClick={() => void signOut()} type="button">
        Sign out
      </button>
      <BandChat />
    </>
  );
}
