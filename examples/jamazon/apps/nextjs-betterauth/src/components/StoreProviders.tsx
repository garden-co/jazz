"use client";

import { Theme } from "@astryxdesign/core";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Center } from "@astryxdesign/core/Center";
import { Spinner } from "@astryxdesign/core/Spinner";
import { jazzTheme } from "@garden-co/design/jazz";
import type { Db } from "jazz-tools";
import { JazzProvider, useDb, useJazzAuth, useSession } from "jazz-tools/react";
import { createContext, useContext, useEffect, useMemo, useRef, type ReactNode } from "react";
import { authClient, requireBetterAuthToken } from "@/src/lib/auth-client";
import { LOCAL_DEFAULTS } from "@/src/lib/build-config.mjs";
import { jazzEnv } from "@/src/lib/jazz-env";
import { claimGuestCart, readGuestCart, type GuestCartLine } from "@/src/store/cart";
import { usePathname } from "next/navigation";
import { isCataloguePath, useCatalogueSnapshot } from "@/src/catalogue/snapshot";
import { SnapshotCatalogue } from "./Catalogue";
import { StoreFrame, StoreShell } from "./StoreShell";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID || LOCAL_DEFAULTS.appId;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL || LOCAL_DEFAULTS.serverUrl;
const origin = process.env.NEXT_PUBLIC_APP_ORIGIN || LOCAL_DEFAULTS.origin;
const GUEST_CART_KEY = "jamazon:guest-cart-to-claim";

// Set while a sign-in, sign-up or sign-out runs, so the restore effect below
// never races it (a racing login would register the identity to a new account
// before sign-up can link it to the guest).
//
// It is module state rather than React state on purpose: it must be visible
// synchronously to an effect that runs mid-transition, without waiting for a
// re-render, and there is one Jazz client per page. `useJazzAuth` does not
// serialise session transitions itself; if it did, this flag could go.
let changingAccount = false;
async function changeAccount(run: () => Promise<void>) {
  changingAccount = true;
  try {
    await run();
  } finally {
    changingAccount = false;
  }
}

/**
 * Everyone who opens Jamazon gets a local-first Jazz account straight away, so
 * browsing and filling a cart work before sign-in and offline. Signing in
 * then attaches a Better Auth identity:
 *
 * - creating an account links it to the guest account (`linkJWT`), so the
 *   cart simply stays where it is;
 * - signing in to an existing account switches accounts, and the guest cart's
 *   lines are claimed into the account's cart.
 */
export function StoreProviders({ children }: { children: ReactNode }) {
  // While the guest's Jazz client opens, the store frame renders straight
  // away, and on catalogue pages the server's copy of the catalogue as soon
  // as it arrives.
  const opening = <OpeningStore />;
  return (
    <Theme theme={jazzTheme} mode="system">
      <JazzProvider
        appId={appId}
        serverUrl={serverUrl}
        env={jazzEnv}
        initial="local-first"
        loading={opening}
        error={(state) => <OpenFailed error={state.error} retry={state.retry} />}
        signedOut={opening}
      >
        <ShopperProvider>
          <StoreShell>{children}</StoreShell>
        </ShopperProvider>
      </JazzProvider>
    </Theme>
  );
}

function OpeningStore() {
  const catalogue = useCatalogueSnapshot();
  const pathname = usePathname();
  const categorySlug = pathname.startsWith("/category/")
    ? decodeURIComponent(pathname.slice("/category/".length))
    : undefined;
  const showsCatalogue = catalogue && isCataloguePath(pathname);
  return (
    <StoreFrame categories={catalogue?.categories ?? []}>
      {showsCatalogue ? (
        <SnapshotCatalogue snapshot={catalogue} categorySlug={categorySlug} />
      ) : (
        <Opening />
      )}
    </StoreFrame>
  );
}

type Shopper = {
  /** The Jazz account that owns the cart and orders. */
  account: string;
  /** Signed in with a Better Auth identity (as opposed to a guest). */
  isSignedIn: boolean;
  name?: string;
  email?: string;
  signIn(email: string, password: string): Promise<void>;
  signUp(name: string, email: string, password: string): Promise<void>;
  signOut(): Promise<void>;
};

const ShopperContext = createContext<Shopper | null>(null);

export function useShopper(): Shopper {
  const shopper = useContext(ShopperContext);
  if (!shopper) throw new Error("useShopper must be used inside <StoreProviders>");
  return shopper;
}

function ShopperProvider({ children }: { children: ReactNode }) {
  const db = useDb();
  const session = useSession();
  const { sessionActions: actions, account: handle } = useJazzAuth();
  const { data: auth, isPending } = authClient.useSession();
  const account = session?.user.account ?? handle?.id ?? "";
  const isSignedIn = handle?.identity.issuer === origin;

  // Restore: a Better Auth session exists (say, after signing in on this
  // browser in another tab) but Jazz still runs the guest account.
  const restoring = useRef(false);
  // Deliberately keyed on the auth state only: `db`, `account` and `actions`
  // change identity as a consequence of the switch this effect starts, and
  // re-running on them would start a second switch.
  useEffect(() => {
    if (isPending || !auth?.user || isSignedIn || restoring.current || changingAccount) return;
    restoring.current = true;
    void switchToAccount(db, account, () => actions.loginOrRegisterJWT({ getToken })).finally(
      () => (restoring.current = false),
    );
  }, [isPending, auth?.user?.id, isSignedIn]);

  // After switching accounts, claim the lines the guest had in their cart.
  // `db` is left out of the deps: it changes with the account, which is.
  useEffect(() => {
    if (!isSignedIn || !account) return;
    const pending = takeGuestCart();
    if (pending.length) void claimGuestCart(db, account, pending);
  }, [isSignedIn, account]);

  const shopper = useMemo<Shopper>(
    () => ({
      account,
      isSignedIn,
      name: auth?.user.name,
      email: auth?.user.email,
      signIn: (email, password) =>
        changeAccount(async () => {
          const result = await authClient.signIn.email({ email, password });
          if (result.error) throw new Error(result.error.message ?? "Could not sign in.");
          await switchToAccount(db, account, () => actions.loginOrRegisterJWT({ getToken }));
        }),
      signUp: (name, email, password) =>
        changeAccount(async () => {
          const result = await authClient.signUp.email({ name, email, password });
          if (result.error)
            throw new Error(result.error.message ?? "Could not create the account.");
          // Linking keeps the guest's Jazz account, and with it the cart.
          await actions.linkJWT({ getToken });
        }),
      signOut: () =>
        changeAccount(async () => {
          await actions.logout();
          await authClient.signOut();
          // A fresh guest account: the next shopper on this device starts empty.
          await actions.createLocalFirst();
        }),
    }),
    [account, isSignedIn, auth?.user.name, auth?.user.email, db, actions],
  );

  return <ShopperContext.Provider value={shopper}>{children}</ShopperContext.Provider>;
}

async function getToken() {
  return requireBetterAuthToken();
}

/** Stash the guest's cart, then switch; the new account claims it on mount. */
async function switchToAccount(db: Db, guestAccount: string, login: () => Promise<void>) {
  const lines = await readGuestCart(db, guestAccount);
  stashGuestCart(lines);
  try {
    await login();
  } catch (error) {
    takeGuestCart();
    throw error;
  }
}

function stashGuestCart(lines: GuestCartLine[]) {
  try {
    if (lines.length) sessionStorage.setItem(GUEST_CART_KEY, JSON.stringify(lines));
  } catch {
    // Without storage the guest cart stays with the guest account.
  }
}

function takeGuestCart(): GuestCartLine[] {
  try {
    const raw = sessionStorage.getItem(GUEST_CART_KEY);
    sessionStorage.removeItem(GUEST_CART_KEY);
    return raw ? (JSON.parse(raw) as GuestCartLine[]) : [];
  } catch {
    return [];
  }
}

function Opening() {
  return (
    <Center height="60vh">
      <Spinner size="lg" label="Opening Jamazon" />
    </Center>
  );
}

function OpenFailed({ error, retry }: { error?: Error; retry: () => Promise<void> }) {
  return (
    <Center height="100vh" padding={4}>
      <Banner
        status="error"
        title="Jamazon could not open"
        description={error?.message ?? "Something went wrong while opening the store."}
        endContent={<Button label="Try again" variant="secondary" onClick={() => void retry()} />}
      />
    </Center>
  );
}
