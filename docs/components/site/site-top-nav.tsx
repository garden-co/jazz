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
import { TopNav, TopNavHeading, TopNavItem } from "@astryxdesign/core/TopNav";
import { JazzLogo } from "@/components/brand/jazz-logo";
import { gitConfig } from "@/lib/layout.shared";
import { SearchField, SearchIconButton, SiteSearch } from "./site-search";

function BrandIcon({ path }: { path: string }) {
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24" fill="currentColor" width="16" height="16">
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
      size="lg"
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
        startContent={
          <>
            {START_LINKS.map((link) => (
              <NavLink key={link.href} {...link} pathname={pathname} />
            ))}
            {/* Below 1120px the links and the field no longer fit side by
              side, so search collapses to an icon. */}
            <span className="site-search-field contents max-[1120px]:hidden">
              <SearchField onOpen={openSearch} />
            </span>
            <span className="contents min-[1120px]:hidden">
              <SearchIconButton onOpen={openSearch} />
            </span>
            {/* Below 900px the switch would crowd the bar; the chosen theme
              still applies. */}
            <span className="contents max-[899px]:hidden">
              <ThemeSwitch />
            </span>
          </>
        }
        endContent={
          <HStack gap={1} vAlign="center" className="site-nav-end">
            {END_LINKS.map((link) => (
              <NavLink key={link.href} {...link} pathname={pathname} />
            ))}
            {/* Below the drawer breakpoint the bar keeps only search and
              the drawer toggle; the social links would push the toggle off screen. */}
            <span className="contents max-md:hidden">
              {SOCIAL.map((link) => (
                <Button
                  key={link.href}
                  label={link.label}
                  href={link.href}
                  target="_blank"
                  rel="noreferrer noopener"
                  variant="ghost"
                  size="sm"
                  isIconOnly
                  icon={<BrandIcon path={link.icon} />}
                />
              ))}
            </span>
          </HStack>
        }
      />
      <SiteSearch isOpen={isSearchOpen} setOpen={setSearchOpen} />
    </>
  );
}
