import { useEffect, type ReactNode } from "react";
import { Theme } from "@astryxdesign/core";
import { LayerProvider } from "@astryxdesign/core/Layer";
import { jazzTheme } from "@garden-co/design/jazz";
import { db } from "./model/db.js";
import { StagePlan } from "./components/StagePlan.js";
import { Loading } from "./components/Loading.js";

export function App() {
  return (
    <Theme theme={jazzTheme} mode="system">
      <LayerProvider>
        <GuestSession>
          <StagePlan />
        </GuestSession>
      </LayerProvider>
    </Theme>
  );
}

/**
 * No sign-up, like the Jazz app's device accounts: a browser without a
 * session signs in as an Instant guest, and Instant keeps that session.
 */
// One guest sign-in, even when React mounts effects twice.
let guestSignIn: Promise<unknown> | undefined;

function GuestSession({ children }: { children: ReactNode }) {
  const { isLoading, user } = db.useAuth();
  const needsGuest = !isLoading && !user;
  useEffect(() => {
    if (needsGuest) guestSignIn ??= db.auth.signInAsGuest();
  }, [needsGuest]);
  return user ? children : <Loading label="Opening StagePlan" />;
}
