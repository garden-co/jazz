"use client";

import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { SiDiscord, SiGithub } from "@icons-pack/react-simple-icons";
import { clsx } from "clsx";
import { useEffect, useState } from "react";

export function HelpLinks({ className }: { className?: string }) {
  const [issueUrl, setIssueUrl] = useState(
    "https://github.com/garden-co/jazz/issues/new?title=Docs%3A%20",
  );

  useEffect(() => {
    if (typeof window !== "undefined") {
      const currentUrl = window.location.href;
      const body = encodeURIComponent(`Page: ${currentUrl}`);
      const fullUrl = `https://github.com/garden-co/jazz/issues/new?template=docs-request.md&body=${body}`;
      setIssueUrl(fullUrl);
    }
  }, []);

  return (
    <div
      className={clsx(
        "not-prose flex flex-wrap items-center gap-x-6",
        className,
      )}
    >
      <Button href={issueUrl} intent="strong" variant="ghost" newTab>
        <SiGithub className="size-4" />
        Docs issue?
      </Button>
      <Button
        href="https://discord.gg/utDMjHYg42"
        intent="strong"
        variant="ghost"
        newTab
        className="lg:mt-3"
      >
        <SiDiscord className="size-4" />
        Join Discord
      </Button>
    </div>
  );
}
