"use client";

import { Banner, Button, Spinner } from "@astryxdesign/core";
import { CenteredPage } from "@/components/centered-page";
import { useEffect, useState } from "react";
import { PosterShopApp } from "@/src/App";
import {
  needsPreparation,
  parseInviteFragment,
  prepareStudio,
  type InviteLink,
} from "@/src/lib/account-enrollment";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";

type Preparation = "pending" | "ready" | "failed";

export default function Dashboard() {
  const { data: session, isPending } = authClient.useSession();
  const userId = session?.user.id;
  // Read once: the fragment is dropped from the address bar after redeeming.
  const [invite] = useState<InviteLink | null>(() =>
    typeof window === "undefined" ? null : parseInviteFragment(window.location.hash),
  );
  const [preparation, setPreparation] = useState<Preparation>("pending");
  const [attempt, setAttempt] = useState(0);
  const [inviteRejected, setInviteRejected] = useState(false);
  useEffect(() => {
    if (!userId) return;
    let cancelled = false;
    // The studio renders from local data straight away; the server only has
    // work to do on a first open (seeding a poster) or for an invite link.
    if (!needsPreparation(userId, invite)) {
      setPreparation("ready");
      return;
    }
    setPreparation("pending");
    void prepareStudio(userId, getJwtFromBetterAuth, invite).then((result) => {
      if (cancelled) return;
      if (result.ok && invite) {
        setInviteRejected(result.inviteRejected);
        // Drop the token from the address bar and history, whether it was
        // redeemed or turned out to be invalid. A transient failure keeps it
        // so "Try again" can redeem it.
        window.history.replaceState(null, "", "/dashboard");
      }
      setPreparation(result.ok ? "ready" : "failed");
    });
    return () => {
      cancelled = true;
    };
  }, [userId, invite, attempt]);
  useEffect(() => {
    // Signed-out visitors (for example from an invite link) sign in first and
    // come back with the same fragment; it never reaches the server.
    if (!isPending && !session) window.location.assign(`/${window.location.hash}`);
  }, [isPending, session]);
  if (!session)
    return (
      <CenteredPage>
        <Spinner label="Preparing your poster studio" />
      </CenteredPage>
    );
  const failure =
    preparation === "failed" ? (
      <Banner
        status="error"
        title="Could not prepare your poster studio"
        description="The server did not finish setting up your poster. Your work is safe; try again."
        endContent={<Button label="Try again" onClick={() => setAttempt(attempt + 1)} />}
      />
    ) : null;
  return (
    <PosterShopApp
      // An invited poster opens as soon as it syncs, while the invite is
      // redeemed in the background; an invalid link falls back to your own.
      initialCanvasId={invite?.canvasId ?? null}
      preparing={preparation === "pending"}
      notice={
        failure ??
        (inviteRejected ? (
          <Banner
            status="info"
            title="This invite link is no longer valid"
            description="It may have been used already or revoked. Ask the poster's admin for a new link. Meanwhile, here is your own studio."
            isDismissable
            onDismiss={() => setInviteRejected(false)}
          />
        ) : undefined)
      }
    />
  );
}
