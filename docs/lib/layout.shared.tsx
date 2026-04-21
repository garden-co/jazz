import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { siDiscord, siGithub, siX } from "simple-icons";
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

const iconClassName = "h-4 w-4";

function SocialIcon({ path }: { path: string }) {
  return (
    <svg aria-hidden="true" className={iconClassName} viewBox="0 0 24 24" fill="currentColor">
      <path d={path} />
    </svg>
  );
}

const githubIcon = <SocialIcon path={siGithub.path} />;
const discordIcon = <SocialIcon path={siDiscord.path} />;
const xIcon = <SocialIcon path={siX.path} />;

export function baseOptions(): BaseLayoutProps {
  return {
    links: [
      {
        text: "Blog",
        url: "/blog",
        active: "nested-url",
      },
      {
        text: "Docs",
        url: "/docs",
        active: "nested-url",
      },
      {
        type: "icon",
        label: "Jazz GitHub",
        text: "GitHub",
        url: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
        icon: githubIcon,
        external: true,
        on: "nav",
      },
      {
        type: "icon",
        label: "Jazz Discord",
        text: "Discord",
        url: "https://discord.gg/RN9UKh52be",
        icon: discordIcon,
        external: true,
        on: "nav",
      },
      {
        type: "icon",
        label: "Jazz on X",
        text: "X",
        url: "https://x.com/jazz_tools",
        icon: xIcon,
        external: true,
        on: "nav",
      },
    ],
    nav: {
      title: navLogo,
    },
  };
}
