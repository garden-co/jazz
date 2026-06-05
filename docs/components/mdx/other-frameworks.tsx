import { Callout } from "fumadocs-ui/components/callout";
import Link from "next/link";

// Shared cross-link for the tier-1 framework pages (React, Svelte, Expo,
// TypeScript). Vue and Solid don't get inline tabs on those pages; this points
// readers to the dedicated per-framework section instead. Defined once so the
// wording stays consistent across every page that uses it.
export function OtherFrameworks() {
  return (
    <Callout type="info">
      Different framework? Vue, Solid, and per-framework guides live in{" "}
      <Link href="/docs/reference/supported-frameworks/overview">Supported Frameworks</Link>.
    </Callout>
  );
}
