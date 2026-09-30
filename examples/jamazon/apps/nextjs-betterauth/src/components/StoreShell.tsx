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
import { app } from "@/schema";
import { useCart } from "@/src/store/cart";
import { useShopper } from "./StoreProviders";

/** The storefront frame: top bar with the cart, category navigation, content. */
export function StoreShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const [menuOpen, setMenuOpen] = useState(false);
  useEffect(() => setMenuOpen(false), [pathname]);
  useStoreBootstrap();
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
          endContent={<TopBarActions />}
        />
      }
      sideNav={<StoreNav pathname={pathname} />}
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
  const { data: categories = [] } = useAll(app.categories.orderBy("position", "asc"));
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
      <SideNavSection title={shopper.isSignedIn ? (shopper.name ?? "Account") : "Account"}>
        <SideNavItem label="Cart" href="/cart" as={Link} isSelected={pathname === "/cart"} />
        <SideNavItem
          label="Orders"
          href="/orders"
          as={Link}
          isSelected={pathname.startsWith("/orders")}
        />
        {shopper.isSignedIn ? (
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
