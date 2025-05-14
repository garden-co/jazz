"use client";

import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { SiDiscord, SiGithub } from "@icons-pack/react-simple-icons";

export function HelpLinks() {
  const issueUrl =
    typeof window !== "undefined"
      ? `https://github.com/garden-co/jazz/issues/new?title=Docs%3A%20&body=${encodeURIComponent(
          `Page: ${window.location.href}`,
        )}`
      : "https://github.com/garden-co/jazz/issues/new?title=Docs%3A%20";

  return (
    <div className="flex not-prose gap-6 md:gap-12">
      <Button
        href={issueUrl}
        variant="plain"
        newTab
        className="inline-flex items-center gap-2 text-sm hover:text-blue p-2 -m-2"
      >
        <SiGithub className="size-4" />
        Report an issue
      </Button>
      <Button
        href="https://discord.gg/utDMjHYg42"
        variant="plain"
        newTab
        className="inline-flex items-center gap-2 text-sm hover:text-blue p-2 -m-2"
      >
        <SiDiscord className="size-4" />
        Join Discord
      </Button>
    </div>
  );
}
