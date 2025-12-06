"use client";

import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { clsx } from "clsx";
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
    <button
      type="button"
      onClick={handleCopy}
      disabled={isLoading}
      className={clsx(
        "flex items-center gap-1.5 text-xs text-stone-600 dark:text-stone-400",
        "hover:text-highlight transition-colors",
        "mb-3 px-2 py-1.5 rounded-md",
        "hover:bg-stone-100 dark:hover:bg-stone-900",
        "disabled:opacity-50 disabled:cursor-not-allowed",
        copied && "text-primary"
      )}
      aria-label="Copy page as Markdown"
    >
      <Icon
        name={copied ? "copySuccess" : "copy"}
        size="xs"
        className={clsx(
          "size-3.5 transition-colors",
          copied ? "stroke-primary" : "stroke-current"
        )}
      />
      <span>{copied ? "Copied!" : "Copy as Markdown"}</span>
    </button>
  );
}
