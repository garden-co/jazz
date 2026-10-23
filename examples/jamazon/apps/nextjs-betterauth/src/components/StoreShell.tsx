"use client";

import { AppShell } from "@astryxdesign/core/AppShell";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { SideNav, SideNavItem, SideNavSection } from "@astryxdesign/core/SideNav";
import { HStack } from "@astryxdesign/core/Stack";
import { TopNav, TopNavHeading } from "@astryxdesign/core/TopNav";
import { useAll } from "jazz-tools/react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState, type ReactNode } from "react";
import { app, type Category } from "@/schema";
import { UNTIL_SERVER_ANSWERS, useCatalogueSnapshot } from "@/src/catalogue/snapshot";
import { useCart } from "@/src/store/cart";
import { useShopper } from "./StoreProviders";

/** The storefront frame: top bar with the cart, category navigation, content. */
export function StoreShell({ children }: { children: ReactNode }) {
  useStoreBootstrap();
  return (
    <StoreChrome actions={<TopBarActions />} nav={(pathname) => <StoreNav pathname={pathname} />}>
      {children}
    </StoreChrome>
  );
}

/**
 * The same frame before the shopper's Jazz client has opened: categories from
 * the server-rendered catalogue, and no account-specific actions yet.
 */
export function StoreFrame({
  categories,
  children,
}: {
  categories: Category[];
  children: ReactNode;
}) {
  return (
    <StoreChrome
      actions={<Button label="Cart" variant="secondary" size="lg" href="/cart" as={Link} />}
      nav={(pathname) => <NavSections pathname={pathname} categories={categories} />}
    >
      {children}
    </StoreChrome>
  );
}

function StoreChrome({
  actions,
  nav,
  children,
}: {
  actions: ReactNode;
  nav: (pathname: string) => ReactNode;
  children: ReactNode;
}) {
  const pathname = usePathname();
  const [menuOpen, setMenuOpen] = useState(false);
  useEffect(() => setMenuOpen(false), [pathname]);
  return (
    <AppShell
      height="auto"
      variant="section"
      contentPadding={0}
      banner={<OfflineBanner />}
      mobileNav={{ isOpen: menuOpen, onOpenChange: setMenuOpen }}
      topNav={
        <TopNav
          label="Jamazon"
          heading={<TopNavHeading heading="Jamazon" headingHref="/" as={Link} />}
          endContent={actions}
        />
      }
      sideNav={nav(pathname)}
    >
      {children}
    </AppShell>
  );
}

function TopBarActions() {
  const shopper = useShopper();
  const { count } = useCart(shopper.account);
  return (
    <HStack gap={2} vAlign="center">
      {!shopper.isSignedIn && (
        <Button label="Sign in" variant="ghost" size="lg" href="/sign-in" as={Link} />
      )}
      <Button
        label={count ? `Cart (${count})` : "Cart"}
        variant="secondary"
        size="lg"
        href="/cart"
        as={Link}
      />
    </HStack>
  );
}

function StoreNav({ pathname }: { pathname: string }) {
  const shopper = useShopper();
  const snapshot = useCatalogueSnapshot();
  const { data: categories } = useAll(
    app.categories.orderBy("position", "asc"),
    UNTIL_SERVER_ANSWERS,
  );
  return (
    <NavSections
      pathname={pathname}
      categories={categories ?? snapshot?.categories ?? []}
      shopper={shopper}
    />
  );
}

function NavSections({
  pathname,
  categories,
  shopper,
}: {
  pathname: string;
  categories: Category[];
  /** Absent while the shopper's Jazz client opens. */
  shopper?: ReturnType<typeof useShopper>;
}) {
  return (
    <SideNav>
      <SideNavSection title="Shop">
        <SideNavItem label="All products" href="/" as={Link} isSelected={pathname === "/"} />
        {categories.map((category) => (
          <SideNavItem
            key={category.id}
            label={category.name}
            href={`/category/${category.slug}`}
            as={Link}
            isSelected={pathname === `/category/${category.slug}`}
          />
        ))}
      </SideNavSection>
      <SideNavSection title={shopper?.isSignedIn ? (shopper.name ?? "Account") : "Account"}>
        <SideNavItem label="Cart" href="/cart" as={Link} isSelected={pathname === "/cart"} />
        <SideNavItem
          label="Orders"
          href="/orders"
          as={Link}
          isSelected={pathname.startsWith("/orders")}
        />
        {!shopper ? null : shopper.isSignedIn ? (
          <SideNavItem label="Sign out" onClick={() => void shopper.signOut()} />
        ) : (
          <SideNavItem
            label="Sign in"
            href="/sign-in"
            as={Link}
            isSelected={pathname === "/sign-in"}
          />
        )}
      </SideNavSection>
    </SideNav>
  );
}

/** Carts keep working offline; say so instead of failing silently. */
function OfflineBanner() {
  const online = useOnline();
  if (online) return null;
  return (
    <Banner
      status="info"
      container="section"
      title="You're offline"
      description="Browsing and your cart keep working on this device and sync when you reconnect. Placing an order needs a connection."
    />
  );
}

function useOnline() {
  const [online, setOnline] = useState(true);
  useEffect(() => {
    const update = () => setOnline(navigator.onLine);
    update();
    window.addEventListener("online", update);
    window.addEventListener("offline", update);
    return () => {
      window.removeEventListener("online", update);
      window.removeEventListener("offline", update);
    };
  }, []);
  return online;
}

/**
 * Ask the server to make sure the catalogue is seeded and the fulfilment
 * worker runs. Idempotent; the catalogue itself arrives through Jazz sync.
 */
function useStoreBootstrap() {
  useEffect(() => {
    void fetch("/api/store", { method: "POST" }).catch(() => {});
  }, []);
}
