import { Badge } from "@astryxdesign/core/Badge";
import Link from "next/link";

/**
 * "Experimental" label for a language that alpha.66 ships as a client only
 * (see `experimental` in `lib/languages`). The build adds one at the top of
 * each of that language's `jazz-environment` tabs and of its setup page; it
 * links to the language's tab on Language Support.
 */
export function ExperimentalBadge({ language }: { language: string }) {
  const tab = language.toLowerCase().replace(/[^a-z0-9]+/g, "-");
  return (
    <p className="not-prose my-2">
      <Link
        href={`/docs/reference/language-support#jazz-environment-${tab}`}
        title={`${language} is experimental: a client of a TypeScript backend. See what it supports.`}
      >
        <Badge label="Experimental" />
      </Link>
    </p>
  );
}
