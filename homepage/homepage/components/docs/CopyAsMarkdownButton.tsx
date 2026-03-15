"use client";

import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";

export function CopyAsMarkdownButton() {
  const [copied, setCopied] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const pathname = usePathname();

  useEffect(() => {
    if (copied) {
      const timeout = setTimeout(() => setCopied(false), 2000);
      return () => clearTimeout(timeout);
    }
  }, [copied]);

  const handleCopy = async () => {
    setIsLoading(true);
    try {
      // Append .md to the current pathname to get the markdown route
      const markdownUrl = `${pathname}.md`;
      
      // Fetch the markdown from the route
      const response = await fetch(markdownUrl);
      if (!response.ok) {
        throw new Error(`Failed to fetch markdown: ${response.statusText}`);
      }

      const markdown = await response.text();

      // Copy to clipboard
      await navigator.clipboard.writeText(markdown);
      setCopied(true);
    } catch (error) {
      console.error("Failed to copy markdown:", error);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Button
      onClick={handleCopy}
      disabled={isLoading}
      intent="strong"
      variant="ghost"
      aria-label="Copy page as Markdown"
    >
      <Icon
        name={copied ? "copySuccess" : "copy"}
        size="xs"
        className="size-4"
      />
      {copied ? "Copied!" : "Copy as Markdown"}
    </Button>
  );
}
