"use client";

import { BandChat } from "@/src/BandChat";
import { useBandChatLifecycle } from "@/components/jazz-provider";
import { authClient } from "@/src/lib/auth-client";

export default function DashboardPage() {
  const { signOut } = useBandChatLifecycle();
  const { data: auth } = authClient.useSession();
  return <BandChat defaultDisplayName={auth?.user.name} onSignOut={() => void signOut()} />;
}
