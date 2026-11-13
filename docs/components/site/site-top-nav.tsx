"use client";

import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import { Monitor, Moon, Sun } from "lucide-react";
import { siDiscord, siGithub, siX } from "simple-icons";
import { Button } from "@astryxdesign/core/Button";
import { HStack } from "@astryxdesign/core/Stack";
import { Icon } from "@astryxdesign/core/Icon";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
import { TopNav, TopNavHeading, TopNavItem, useTopNavRenderMode } from "@astryxdesign/core/TopNav";
import { JazzLogo } from "@/components/brand/jazz-logo";
import { gitConfig } from "@/lib/layout.shared";
import { SearchField, SearchIconButton, SiteSearch } from "./site-search";

function BrandIcon({ path }: { path: string }) {
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24" fill="currentColor" width="20" height="20">
      <path d={path} />
    </svg>
  );
}

const THEMES = [
  { value: "light", label: "Light", icon: Sun },
  { value: "system", label: "System", icon: Monitor },
  { value: "dark", label: "Dark", icon: Moon },
];

function ThemeSwitch() {
  const { theme, setTheme } = useTheme();
  // The stored theme is unknown on the server; show "system" until mounted so
  // hydration matches.
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  return (
    <SegmentedControl
      label="Colour theme"
      size="md"
      // Subtle next to the social links: no filled track behind the options.
      style={{ backgroundColor: "transparent" }}
      value={mounted ? (theme ?? "system") : "system"}
      onChange={setTheme}
    >
      {THEMES.map((option) => (
        <SegmentedControlItem
          key={option.value}
          value={option.value}
          label={option.label}
          isLabelHidden
          icon={<Icon icon={option.icon} size="sm" />}
        />
      ))}
    </SegmentedControl>
  );
}

const SOCIAL = [
  {
    label: "Jazz on GitHub",
    href: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
    icon: siGithub.path,
  },
  { label: "Jazz on Discord", href: "https://discord.gg/RN9UKh52be", icon: siDiscord.path },
  { label: "Jazz on X", href: "https://x.com/jazz_tools", icon: siX.path },
];

const START_LINKS = [
  { label: "Pricing", href: "/#pricing" },
  { label: "Examples & Benches", href: "/examples" },
  { label: "Documentation", href: "/docs" },
];
const END_LINKS = [{ label: "Blog", href: "/blog" }];

function NavLink({ label, href, pathname }: { label: string; href: string; pathname: string }) {
  return (
    <TopNavItem
      label={label}
      href={href}
      // Labels stay on one line; the bar collapses search instead.
      className="whitespace-nowrap"
      isSelected={pathname === href || pathname.startsWith(`${href}/`)}
    />
  );
}

/** The start links; in the mobile drawer the end links join them. */
function StartLinks({ pathname }: { pathname: string }) {
  const mode = useTopNavRenderMode();
  const links = mode === "drawer" ? [...START_LINKS, ...END_LINKS] : START_LINKS;
  return (
    <>
      {links.map((link) => (
        <NavLink key={link.href} {...link} pathname={pathname} />
      ))}
    </>
  );
}

/** Search in the centre of the full bar. The mobile bar has its own icon, so
  the drawer leaves it out. */
function CenterSearch({ onOpen }: { onOpen: () => void }) {
  if (useTopNavRenderMode() === "drawer") return null;
  return (
    <>
      {/* Below 1360px the centred field would collide with the links, so
        search collapses to an icon in the end slot (see EndContent). */}
      <span className="site-search-field site-search-wide contents">
        <SearchField onOpen={onOpen} />
      </span>
    </>
  );
}

function EndContent({ pathname, onOpen }: { pathname: string; onOpen: () => void }) {
  // The mobile bar keeps only search; the links move into the drawer.
  if (useTopNavRenderMode() === "mobile-bar") {
    return (
      <HStack gap={2} vAlign="center" className="site-nav-end">
        <SearchIconButton onOpen={onOpen} />
      </HStack>
    );
  }
  return (
    <HStack gap={2} vAlign="center" className="site-nav-end">
      {/* The centred slot is absolutely positioned, so an icon there sits on
        top of the start links; below 1360px it lives here instead. */}
      <span className="site-search-narrow contents">
        <SearchIconButton onOpen={onOpen} />
      </span>
      {END_LINKS.map((link) => (
        <NavLink key={link.href} {...link} pathname={pathname} />
      ))}
      <span className="contents">
        {SOCIAL.map((link) => (
          <Button
            key={link.href}
            label={link.label}
            href={link.href}
            target="_blank"
            rel="noreferrer noopener"
            variant="ghost"
            size="md"
            isIconOnly
            icon={<BrandIcon path={link.icon} />}
          />
        ))}
      </span>
      {/* Below 900px the switch would crowd the bar; the chosen theme
        still applies. */}
      <span className="contents max-[899px]:hidden">
        <ThemeSwitch />
      </span>
    </HStack>
  );
}

/** The jazz.tools header shared by the homepage, blog and docs. */
export function SiteTopNav() {
  const pathname = usePathname();
  const [isSearchOpen, setSearchOpen] = useState(false);
  const openSearch = () => setSearchOpen(true);
  // Other Jazz surfaces (the Cloud dashboard) link to `?search` to open this palette.
  useEffect(() => {
    const url = new URL(window.location.href);
    if (!url.searchParams.has("search")) return;
    setSearchOpen(true);
    url.searchParams.delete("search");
    window.history.replaceState(null, "", url);
  }, []);
  return (
    <>
      <TopNav
        label="Main"
        heading={
          <TopNavHeading
            logo={<JazzLogo className="h-[1.8rem] w-auto" label="Jazz home" />}
            headingHref="/"
          />
        }
        startContent={<StartLinks pathname={pathname} />}
        centerContent={<CenterSearch onOpen={openSearch} />}
        endContent={<EndContent pathname={pathname} onOpen={openSearch} />}
      />
      <SiteSearch isOpen={isSearchOpen} setOpen={setSearchOpen} />
    </>
  );
}
