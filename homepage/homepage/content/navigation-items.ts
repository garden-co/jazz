import { NavItemProps } from "@garden-co/design-system/src/components/organisms/Nav";

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
