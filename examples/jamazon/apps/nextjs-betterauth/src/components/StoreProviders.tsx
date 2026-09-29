"use client";

import { Theme } from "@astryxdesign/core";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Center } from "@astryxdesign/core/Center";
import { Spinner } from "@astryxdesign/core/Spinner";
import { jazzTheme } from "@garden-co/design/jazz";
import { JazzProvider, useDb, useJazzAuth, useSession } from "jazz-tools/react";
import { createContext, useContext, useEffect, useMemo, useRef, type ReactNode } from "react";
import { authClient, requireBetterAuthToken } from "@/src/lib/auth-client";
import { claimGuestCart, readGuestCart, type GuestCartLine } from "@/src/store/cart";
import { StoreShell } from "./StoreShell";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? "jamazon-local";
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "http://127.0.0.1:4200";
const origin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";
const GUEST_CART_KEY = "jamazon:guest-cart-to-claim";

// Set while a sign-in, sign-up or sign-out runs, so the restore effect below
// never races it (a racing login would register the identity to a new account
// before sign-up can link it to the guest).
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
  return (
    <Theme theme={jazzTheme} mode="system">
      <JazzProvider
        appId={appId}
        serverUrl={serverUrl}
        initial="local-first"
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
  const { sessionActions: actions, account: handle } = useJazzAuth();
  const { data: auth, isPending } = authClient.useSession();
  const account = session?.user.account ?? handle?.id ?? "";
  const isSignedIn = handle?.identity.issuer === origin;

  // Restore: a Better Auth session exists (say, after signing in on this
  // browser in another tab) but Jazz still runs the guest account.
  const restoring = useRef(false);
  useEffect(() => {
    if (isPending || !auth?.user || isSignedIn || restoring.current || changingAccount) return;
    restoring.current = true;
    void switchToAccount(db, account, () => actions.loginOrRegisterJWT({ getToken })).finally(
      () => (restoring.current = false),
    );
  }, [isPending, auth?.user?.id, isSignedIn]);

  // After switching accounts, claim the lines the guest had in their cart.
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
          if (result.error) throw new Error(result.error.message ?? "Could not create the account.");
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
async function switchToAccount(
  db: ReturnType<typeof useDb>,
  guestAccount: string,
  login: () => Promise<void>,
) {
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
