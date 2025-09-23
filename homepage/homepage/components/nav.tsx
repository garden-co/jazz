import { ThemeToggle } from "@/components/ThemeToggle";
import { QuickSearch } from "@/components/quick-search";
import { navigationItems } from "@/content/navigation-items";
import { socials } from "@/content/socials";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { JazzLogo } from "@garden-co/design-system/src/components/atoms/logos/JazzLogo";
import {
  MobileNav,
  Nav,
  type NavSection,
} from "@garden-co/design-system/src/components/organisms/Nav";

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
      cta={<div className="flex items-center gap-3 md:mr-2">
        <QuickSearch />
        <Button
          intent="primary"
          href="https://dashboard.jazz.tools"
          newTab
        >
          Dashboard
        </Button>
      </div>
    }
    ></Nav>
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
      cta={<div className="flex items-center gap-2">
        <QuickSearch />
        <Button
          intent="primary"
          href="https://dashboard.jazz.tools"
          newTab
        >
          Dashboard
        </Button>
      </div>}
    />
  );
}
