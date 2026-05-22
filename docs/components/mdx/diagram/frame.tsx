import type { ReactNode } from "react";

// Shared chrome for diagrams. Portable CSS via --diagram-* (no fd-/Tailwind);
// `not-prose` is the one deliberate host-typography seam so MDX prose styles
// don't bleed into the diagram — harmless on non-Tailwind hosts.
export function DiagramFrame({
  eyebrow,
  description,
  children,
  responsive = false,
}: {
  eyebrow: string;
  description: ReactNode;
  children: ReactNode;
  // false (default): desktop-only (adds `dg-frame--desktop-only`). true
  // (Graph/Sequence): one responsive definition, static below the threshold.
  responsive?: boolean;
}) {
  return (
    <div
      className={"diagram-host dg-frame not-prose" + (responsive ? "" : " dg-frame--desktop-only")}
    >
      <p className="dg-frame-eyebrow">{eyebrow}</p>
      <p className="dg-frame-desc">{description}</p>
      {children}
    </div>
  );
}
