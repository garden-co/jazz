import { ThemeToggle } from "@/components/ThemeToggle";
import { socials } from "@/content/socials";
import { navigationItems } from "@/lib/navigation-items";
import { JazzLogo } from "gcmp-design-system/src/app/components/atoms/logos/JazzLogo";
import {
  MobileNav,
  Nav,
  type NavSection,
} from "gcmp-design-system/src/app/components/organisms/Nav";

export function JazzNav({
  sections,
  hideMobileNav,
}: { sections?: NavSection[]; hideMobileNav?: boolean }) {
  return (
    <Nav
      sections={sections}
      mainLogo={<JazzLogo className="w-20 md:w-24" />}
      themeToggle={ThemeToggle}
      items={navigationItems}
      socials={socials}
      hideMobileNav={hideMobileNav}
    />
  );
}

export function JazzMobileNav({ sections }: { sections?: NavSection[] }) {
  return (
    <MobileNav
      navBarClassName="absolute top-0 w-full left-0"
      sections={sections}
      mainLogo={<JazzLogo className="w-20 md:w-24" />}
      themeToggle={ThemeToggle}
      items={navigationItems}
      socials={socials}
    />
  );
}
