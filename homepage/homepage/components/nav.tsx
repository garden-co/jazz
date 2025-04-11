import { ThemeToggle } from "@/components/ThemeToggle";
import { socials } from "@/content/socials";
import { navigationItems } from "@/lib/navigation-items";
import { JazzLogo } from "gcmp-design-system/src/app/components/atoms/logos/JazzLogo";
import {
  MobileNav,
  Nav,
  type NavSection,
} from "gcmp-design-system/src/app/components/organisms/Nav";

export function JazzNav({ sections }: { sections?: NavSection[] }) {
  return (
    <Nav
      sections={sections}
      mainLogo={<JazzLogo className="w-20 md:w-24" />}
      themeToggle={ThemeToggle}
      items={navigationItems}
      socials={socials}
    />
  );
}

export function JazzMobileNav({ sections }: { sections?: NavSection[] }) {
  return (
    <MobileNav
      sections={sections}
      mainLogo={<JazzLogo className="w-20 md:w-24" />}
      themeToggle={ThemeToggle}
      items={navigationItems}
      socials={socials}
    />
  );
}
