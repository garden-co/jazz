"use client";

import { BandChat } from "@/src/BandChat";
import { useBandChatLifecycle } from "@/components/jazz-provider";

export default function DashboardPage() {
  const { signOut } = useBandChatLifecycle();
  async function handleSignOut() {
    await signOut();
  }
  return (
    <>
      <button className="sign-out" onClick={() => void handleSignOut()} type="button">
        Sign out
      </button>
      <BandChat />
    </>
  );
}
