"use client";

import { Theme } from "@astryxdesign/core";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Center } from "@astryxdesign/core/Center";
import { Spinner } from "@astryxdesign/core/Spinner";
import { jazzTheme } from "@garden-co/design/jazz";
import type { Db } from "jazz-tools";
import { JazzProvider, useDb, useJazzSession, useSession } from "jazz-tools/react";
import { createContext, useContext, useEffect, useMemo, useRef, type ReactNode } from "react";
import { authClient } from "@/src/lib/auth-client";
import { claimGuestCart, readGuestCart, type GuestCartLine } from "@/src/store/cart";
import { StoreShell } from "./StoreShell";

// Start on the thin client; load the local-first one (WASM, local storage,
// offline writes) in its own chunk once the page has loaded.
const loadLocalFirst = () => import("jazz-tools/local-first");

const GUEST_CART_KEY = "jamazon:guest-cart-to-claim";

// Set while a sign-in, sign-up or sign-out runs, so the restore effect below
// never races it (a racing login would register the identity to a new account
// before sign-up can link it to the guest).
//
// It is module state rather than React state on purpose: it must be visible
// synchronously to an effect that runs mid-transition, without waiting for a
// re-render, and there is one Jazz client per page. `useJazzSession` does not
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
 * Everyone who opens Jamazon gets a Jazz device account straight away, so
 * browsing and filling a cart work before sign-in and offline. Signing in
 * then attaches a Better Auth identity through the store's backend
 * (`createJazz` in src/server/backend.ts), which reads the Better Auth session:
 *
 * - creating an account links it to the guest account (`linkDeviceAccount`), so
 *   the cart simply stays where it is;
 * - signing in to an existing account switches accounts, and the guest cart's
 *   lines are claimed into the account's cart.
 */
export function StoreProviders({ children }: { children: ReactNode }) {
  return (
    <Theme theme={jazzTheme} mode="system">
      <JazzProvider
        config={{ serverUrl: "/jazz", client: loadLocalFirst, initial: "device" }}
        loading={<Opening />}
        error={(state) => <OpenFailed error={state.error} retry={state.retry} />}
        signedOut={<Opening />}
      >
        <ShopperProvider>
          <StoreShell>{children}</StoreShell>
        </ShopperProvider>
      </JazzProvider>
    </Theme>
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
  const jazz = useJazzSession();
  const { user, refresh, linkDeviceAccount, createDeviceAccount } = jazz;
  const signOutOfJazz = jazz.signOut;
  const { data: auth, isPending, error: authError } = authClient.useSession();
  const account = session?.user ?? user?.accountId ?? "";
  // Signed in: an account the backend names, not this browser's guest.
  const isSignedIn = user?.kind === "backend";

  // Restore: a Better Auth session exists (say, after signing in on this
  // browser in another tab) but Jazz still runs the guest account.
  const restoring = useRef(false);
  // Deliberately keyed on the auth state only: `db` and `account`
  // change identity as a consequence of the switch this effect starts, and
  // re-running on them would start a second switch.
  //
  // After a reload Jazz reopens as a guest (a signed-in account is not kept on
  // the device), so this also signs a returning shopper back in.
  //
  // Failures are not rethrown: Jazz reports them in its lifecycle state, which
  // shows the error fallback with a retry. The `restoring` guard also keeps
  // StrictMode's second effect run from starting a second, "already pending"
  // operation.
  useEffect(() => {
    if (isPending || restoring.current || changingAccount) return;
    const run = (operation: () => Promise<void>) => {
      restoring.current = true;
      void operation()
        .catch(() => {})
        .finally(() => (restoring.current = false));
    };
    if (!auth?.user) {
      // Signed out of Better Auth (say, in another tab) while this browser
      // still runs the shopper's account: start a fresh guest instead of
      // writing to an account that can no longer sync. Skip it when the
      // session could not be read at all (offline), so the shopper keeps
      // their cart.
      if (isSignedIn && !authError)
        run(() =>
          changeAccount(async () => {
            await signOutOfJazz();
            await createDeviceAccount();
          }),
        );
      return;
    }
    if (isSignedIn) return;
    run(() => switchToAccount(db, account, refresh));
  }, [isPending, auth?.user?.id, !!authError, isSignedIn]);

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
          await switchToAccount(db, account, refresh);
        }),
      signUp: (name, email, password) =>
        changeAccount(async () => {
          const result = await authClient.signUp.email({ name, email, password });
          if (result.error)
            throw new Error(result.error.message ?? "Could not create the account.");
          // Linking keeps the guest's Jazz account, and with it the cart.
          await linkDeviceAccount();
        }),
      signOut: () =>
        changeAccount(async () => {
          await signOutOfJazz();
          await authClient.signOut();
          // A fresh guest account: the next shopper on this device starts empty.
          await createDeviceAccount();
        }),
    }),
    [
      account,
      isSignedIn,
      auth?.user.name,
      auth?.user.email,
      db,
      refresh,
      linkDeviceAccount,
      signOutOfJazz,
      createDeviceAccount,
    ],
  );

  return <ShopperContext.Provider value={shopper}>{children}</ShopperContext.Provider>;
}

/** Stash the guest's cart, then switch; the new account claims it on mount. */
async function switchToAccount(db: Db, guestAccount: string, login: () => Promise<unknown>) {
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
    <Center height="100vh">
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
