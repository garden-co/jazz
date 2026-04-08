import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { JazzLogo } from "@/components/brand/jazz-logo";

// fill this with your actual GitHub info, for example:
export const gitConfig = {
  user: "garden-co",
  repo: "jazz2",
  branch: "main",
};

// Fumadocs serializes `nav.title` into a client layout prop, so this needs to
// be plain JSX rather than a component reference.
const navLogo = JazzLogo({ className: "h-6 w-auto", label: "Jazz home" });

export function baseOptions(): BaseLayoutProps {
  return {
    links: [
      {
        text: "Docs",
        url: "/docs",
        active: "nested-url",
      },
    ],
    nav: {
      title: navLogo,
    },
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}
