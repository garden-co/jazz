"use client";

import type { CSSProperties, ReactNode, Ref } from "react";

// Composable node slots. The engine ships unopinionated, token-driven pieces;
// each diagram definition assembles them (and drops in fully bespoke content
// where a card has no shared shape, e.g. Lens's ClientDevice). Size/spacing is
// the consumer's via `style`/`className`. className passes through so a host
// (docs) may layer its own utilities — the engine itself uses none.

function cx(...parts: Array<string | undefined | false>): string {
  return parts.filter(Boolean).join(" ");
}

export function NodeShell({
  ref,
  className,
  style,
  children,
}: {
  ref?: Ref<HTMLDivElement>;
  className?: string;
  style?: CSSProperties;
  children: ReactNode;
}) {
  return (
    <div ref={ref} className={cx("dg-node", className)} style={style}>
      {children}
    </div>
  );
}

export function NodeIcon({ children }: { children: ReactNode }) {
  return <span className="dg-node-icon">{children}</span>;
}

export function NodeTitle({ className, children }: { className?: string; children: ReactNode }) {
  return <div className={cx("dg-node-title", className)}>{children}</div>;
}

export function NodeSubtitle({ children }: { children: ReactNode }) {
  return <div className="dg-node-subtitle">{children}</div>;
}

export function NodeFooter({
  className,
  style,
  children,
}: {
  className?: string;
  style?: CSSProperties;
  children: ReactNode;
}) {
  return (
    <div className={cx("dg-node-footer", className)} style={style}>
      {children}
    </div>
  );
}

export function NodeAction({
  onClick,
  className,
  "aria-label": ariaLabel,
  children,
}: {
  onClick?: () => void;
  className?: string;
  "aria-label"?: string;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-label={ariaLabel}
      className={cx("dg-node-action", className)}
    >
      {children}
    </button>
  );
}
