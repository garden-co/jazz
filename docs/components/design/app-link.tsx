"use client";

import type { ComponentProps } from "react";
import NextLink from "next/link";
import { Link } from "@astryxdesign/core/Link";

/** Astryx `Link` that routes internal hrefs through Next.js client navigation. */
export function AppLink(props: Omit<ComponentProps<typeof Link>, "as">) {
  return <Link {...props} as={NextLink} />;
}
