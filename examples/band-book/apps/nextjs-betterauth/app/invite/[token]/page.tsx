"use client";

import { use, useEffect, useState } from "react";
import { Button } from "@astryxdesign/core";
import { StatusScreen } from "@/components/status-screen";
import { useSession } from "jazz-tools/react";
import { redeemInviteLink } from "@/src/lib/server-calls";

/**
 * Signed-out visitors see the sign-in form here (from the Jazz provider) and
 * stay on this page, so signing in redeems the invite and opens the page.
 */
export default function InvitePage({ params }: { params: Promise<{ token: string }> }) {
  const { token } = use(params);
  const account = useSession()?.user.account;
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!account) return;
    void (async () => {
      const response = await redeemInviteLink(token).catch((cause: unknown) => cause);
      if (!(response instanceof Response))
        return setError(response instanceof Error ? response.message : String(response));
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
  }, [account, token]);

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
