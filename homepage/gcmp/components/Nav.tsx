import { ThemeToggle } from "@/components/ThemeToggle";
import { Button } from "gcmp-design-system/src/app/components/atoms/Button";
import { GcmpLogo } from "gcmp-design-system/src/app/components/atoms/logos/GcmpLogo";
import { Nav } from "gcmp-design-system/src/app/components/organisms/Nav";
export function GcmpNav() {
  const cta = (
    <Button variant="secondary" className="ml-3" href="mailto:hello@garden.co">
      Contact us
    </Button>
  );
  return (
    <Nav
      mainLogo={<GcmpLogo className="h-10 w-auto" />}
      items={[
        { title: "Theses & Products", href: "/" },
        { title: "Blog", href: "/news" },
        { title: "Team", href: "/team" },
      ]}
      cta={cta}
      themeToggle={ThemeToggle}
      socials={{
        bluesky: "https://bsky.app/profile/garden.co",
        x: "https://x.com/gardendotco",
        github: "https://github.com/garden-co",
      }}
    ></Nav>
  );
}
