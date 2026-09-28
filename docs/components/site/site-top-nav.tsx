"use client";

import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import { Moon, Sun } from "lucide-react";
import { siDiscord, siGithub, siX } from "simple-icons";
import { Button } from "@astryxdesign/core/Button";
import { HStack } from "@astryxdesign/core/Stack";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";
import { TopNav, TopNavHeading, TopNavItem } from "@astryxdesign/core/TopNav";
import { JazzLogo } from "@/components/brand/jazz-logo";
import { gitConfig } from "@/lib/layout.shared";
import { SiteSearch } from "./site-search";

function BrandIcon({ path }: { path: string }) {
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24" fill="currentColor" width="16" height="16">
      <path d={path} />
    </svg>
  );
}

function ThemeToggle() {
  const { resolvedTheme, setTheme } = useTheme();
  // The stored theme is unknown on the server; render the light-mode control
  // until mounted so hydration matches.
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  const isDark = mounted && resolvedTheme === "dark";
  return (
    <IconButton
      label={isDark ? "Switch to light mode" : "Switch to dark mode"}
      variant="ghost"
      size="sm"
      icon={<Icon icon={isDark ? Sun : Moon} size="sm" />}
      onClick={() => setTheme(isDark ? "light" : "dark")}
    />
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

const LINKS = [
  { label: "Blog", href: "/blog" },
  { label: "Docs", href: "/docs" },
];

/** The jazz.tools header shared by the homepage, blog and docs. */
export function SiteTopNav() {
  const pathname = usePathname();
  return (
    <TopNav
      label="Main"
      heading={
        <TopNavHeading
          logo={<JazzLogo className="h-6 w-auto" label="Jazz home" />}
          headingHref="/"
        />
      }
      startContent={
        <>
          {LINKS.map((link) => (
            <TopNavItem
              key={link.href}
              label={link.label}
              href={link.href}
              isSelected={pathname === link.href || pathname.startsWith(`${link.href}/`)}
            />
          ))}
          <TopNavItem label="Dashboard" href="https://v2.dashboard.jazz.tools" />
        </>
      }
      endContent={
        <HStack gap={1} vAlign="center">
          <SiteSearch />
          {/* Below the drawer breakpoint the bar keeps only search, theme and
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
          <ThemeToggle />
        </HStack>
      }
    />
  );
}
