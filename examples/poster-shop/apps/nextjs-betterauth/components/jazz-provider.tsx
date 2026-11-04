"use client";

import { Banner, Button, Spinner } from "@astryxdesign/core";
import { CenteredPage } from "@/components/centered-page";
import { JazzProvider as Jazz } from "jazz-tools/react";

// Start on the thin client; load the local-first one (WASM, local storage,
// offline writes) in its own chunk once the page has loaded.
const loadLocalFirst = () => import("jazz-tools/local-first");

// Better Auth signs users in; PosterShop's backend (`createJazz` in
// src/lib/jazz.server.ts) reads its session in `authenticate`.
/**
 * Signed-in visitors get the studio's Jazz client; signed-out ones see the
 * page as is (the sign-in form, which needs no Jazz).
 */
export function JazzProvider({ children }: { children: React.ReactNode }) {
  return (
    <Jazz
      serverUrl="/jazz"
      client={loadLocalFirst}
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
