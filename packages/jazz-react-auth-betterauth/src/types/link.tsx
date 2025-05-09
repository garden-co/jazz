import type { ComponentType, ReactNode } from "react";

export type Link = ComponentType<{
  href: any;
  className?: string;
  children: ReactNode;
  target?: string;
  rel?: string;
}>;

export const DefaultLink: Link = ({
  href,
  className,
  children,
  target,
  rel,
}) => (
  <a className={className} href={href} target={target} rel={rel}>
    {children}
  </a>
);
