import { NavItemProps } from "gcmp-design-system/src/app/components/organisms/Nav";

export const navigationItems: NavItemProps[] = [
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
    href: "/status",
    title: "Status",
  },
];
