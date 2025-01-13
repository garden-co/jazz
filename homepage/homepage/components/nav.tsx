import { ThemeToggle } from "@/components/ThemeToggle";
import { socials } from "@/lib/socials";
import { useFramework } from "@/lib/use-framework";
import { JazzLogo } from "gcmp-design-system/src/app/components/atoms/logos/JazzLogo";
import {
  Nav,
  type NavSection,
} from "gcmp-design-system/src/app/components/organisms/Nav";

export function JazzNav({ sections }: { sections?: NavSection[] }) {
  return (
    <Nav
      sections={sections}
      mainLogo={<JazzLogo className="w-20 md:w-24" />}
      themeToggle={ThemeToggle}
      items={[
        { title: "Jazz Cloud", href: "/cloud" },
        {
          title: "Docs",
          href: "/docs",
          items: [],
        },
        {
          title: "Examples",
          href: "/examples",
        },
        {
          title: "API ref",
          href: "/api-reference",
        },
        {
          title: "Built with Jazz",
          href: "/showcase",
        },
        {
          title: "Blog",
          href: "https://garden.co/news",
          firstOnRight: true,
          newTab: true,
        },
        {
          title: "Releases",
          href: "https://github.com/garden-co/jazz/releases",
          newTab: true,
        },
        {
          title: "Status",
          href: "/status",
        },
      ]}
      socials={socials}
    />
  );
}
