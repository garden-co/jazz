"use client";

import { use, useEffect, useState } from "react";
import { Button } from "@astryxdesign/core";
import { StatusScreen } from "@/components/status-screen";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";
import { redeemInviteLink } from "@/src/lib/account-enrollment";

/** Signed out: sign in first and come back. Signed in: redeem, then open the shared page. */
export default function InvitePage({ params }: { params: Promise<{ token: string }> }) {
  const { token } = use(params);
  const { data: session, isPending } = authClient.useSession();
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (isPending) return;
    if (!session) {
      window.location.assign(`/?next=${encodeURIComponent(`/invite/${token}`)}`);
      return;
    }
    void (async () => {
      const jwt = await getJwtFromBetterAuth();
      if (!jwt) return setError("Your session has no Jazz token. Sign in again.");
      const response = await redeemInviteLink(jwt, token);
      if (!response.ok)
        return setError(
          response.status === 404
            ? "This invite link was revoked or does not exist."
            : `The server answered ${response.status}.`,
        );
      const { workspaceId, pageId } = (await response.json()) as {
        workspaceId: string;
        pageId: string | null;
      };
      const search = new URLSearchParams({ w: workspaceId });
      if (pageId) search.set("p", pageId);
      window.location.assign(`/workspace?${search.toString()}`);
    })();
  }, [isPending, session?.user.id, token]);

  if (error)
    return (
      <StatusScreen
        label="Could not accept the invite"
        error={error}
        action={<Button label="Go to your workspace" href="/workspace" />}
      />
    );
  return <StatusScreen label="Accepting invite" />;
}
