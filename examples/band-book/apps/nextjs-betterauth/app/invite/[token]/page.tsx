"use client";

import { use, useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
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
  const router = useRouter();
  const [error, setError] = useState<string | null>(null);
  // Redeem once per token, even when StrictMode runs the effect twice.
  const redeemed = useRef<string | null>(null);

  useEffect(() => {
    if (!account || redeemed.current === token) return;
    redeemed.current = token;
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
      // A client-side navigation: the Jazz client in the root layout stays
      // open, so the workspace renders from it instead of booting Jazz again.
      router.replace(`/workspace?${search.toString()}`);
    })();
  }, [account, router, token]);

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
