import type { ReactNode } from "react";

// Shared chrome for MDX diagrams: hidden on small screens (mobile gets a
// static fallback), wrapped in a rounded card with the eyebrow + description
// styling that every diagram uses.
export function DiagramFrame({
  eyebrow,
  description,
  children,
}: {
  eyebrow: string;
  description: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="my-6 rounded-xl border border-fd-border bg-fd-card/30 p-5 not-prose hidden lg:block">
      <p className="uppercase text-xs font-bold text-fd-primary">{eyebrow}</p>
      <p className="text-sm text-fd-muted-foreground mb-6">{description}</p>
      {children}
    </div>
  );
}
