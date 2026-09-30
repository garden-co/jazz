"use client";

import { Banner, Button, Spinner } from "@astryxdesign/core";
import { CenteredPage } from "@/components/centered-page";
import { useEffect, useState } from "react";
import { PosterShopApp } from "@/src/App";
import { parseInviteFragment, prepareStudio } from "@/src/lib/account-enrollment";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";

export default function Dashboard() {
  const { data: session, isPending } = authClient.useSession();
  const [bootstrap, setBootstrap] = useState<"loading" | "ready" | "failed">("loading");
  const [attempt, setAttempt] = useState(0);
  const [joinedCanvasId, setJoinedCanvasId] = useState<string | null>(null);
  const [inviteRejected, setInviteRejected] = useState(false);
  const userId = session?.user.id;
  useEffect(() => {
    if (!userId) return;
    let cancelled = false;
    setBootstrap("loading");
    const invite = parseInviteFragment(window.location.hash);
    void prepareStudio(userId, getJwtFromBetterAuth, invite).then((result) => {
      if (cancelled) return;
      if (result.ok && invite) {
        setJoinedCanvasId(result.joinedCanvasId);
        setInviteRejected(result.inviteRejected);
        // Drop the token from the address bar and history, whether it was
        // redeemed or turned out to be invalid. A transient failure keeps it
        // so "Try again" can redeem it.
        window.history.replaceState(null, "", "/dashboard");
      }
      setBootstrap(result.ok ? "ready" : "failed");
    });
    return () => {
      cancelled = true;
    };
  }, [userId, attempt]);
  useEffect(() => {
    // Signed-out visitors (for example from an invite link) sign in first and
    // come back with the same fragment; it never reaches the server.
    if (!isPending && !session) window.location.assign(`/${window.location.hash}`);
  }, [isPending, session]);
  if (!session || bootstrap === "loading")
    return (
      <CenteredPage>
        <Spinner label="Preparing your poster studio" />
      </CenteredPage>
    );
  if (bootstrap === "failed")
    return (
      <CenteredPage>
        <Banner
          status="error"
          title="Could not prepare your poster studio"
          description="The server did not finish setting up your first poster. Your work is safe; try again."
          endContent={<Button label="Try again" onClick={() => setAttempt(attempt + 1)} />}
        />
      </CenteredPage>
    );
  return (
    <PosterShopApp
      initialCanvasId={joinedCanvasId}
      notice={
        inviteRejected ? (
          <Banner
            status="info"
            title="This invite link is no longer valid"
            description="It may have been used already or revoked. Ask the poster's admin for a new link. Meanwhile, here is your own studio."
            isDismissable
            onDismiss={() => setInviteRejected(false)}
          />
        ) : undefined
      }
    />
  );
}
