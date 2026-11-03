"use client";

import { Banner, Button, Spinner } from "@astryxdesign/core";
import { CenteredPage } from "@/components/centered-page";
import { JazzProvider as Jazz, betterAuth } from "jazz-tools/react";
import { authClient } from "@/src/lib/auth-client";
import { LOCAL_DEFAULTS } from "@/src/lib/build-config.mjs";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID || LOCAL_DEFAULTS.appId;
// Better Auth's session drives sign-in; PosterShop's backend (`createJazz` in
// src/lib/jazz.server.ts) reads it in `authenticate`.
const auth = betterAuth(authClient);

/**
 * Signed-in visitors get the studio's Jazz client; signed-out ones see the
 * page as is (the sign-in form, which needs no Jazz).
 */
export function JazzProvider({ children }: { children: React.ReactNode }) {
  return (
    <Jazz
      appId={appId}
      serverUrl="/jazz"
      auth={auth}
      signedOut={children}
      loading={
        <CenteredPage>
          <Spinner label="Opening the poster studio" />
        </CenteredPage>
      }
      error={(state) => (
        <CenteredPage>
          <Banner
            status="error"
            title="Could not open the poster studio"
            description={state.error?.message}
            endContent={<Button label="Try again" onClick={() => void state.retry()} />}
          />
        </CenteredPage>
      )}
    >
      {children}
    </Jazz>
  );
}
