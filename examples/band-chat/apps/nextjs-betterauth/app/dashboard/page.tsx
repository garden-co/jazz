"use client";
import { BandChat } from "@/src/App";
import { authClient } from "@/src/lib/auth-client";
export default function DashboardPage() {
  async function signOut() {
    await authClient.signOut();
    window.location.assign("/");
  }
  return (
    <>
      <button type="button" onClick={signOut}>
        Sign out
      </button>
      <BandChat />
    </>
  );
}
