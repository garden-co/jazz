"use client";

import { type ReactNode, useRef } from "react";

interface HeadingProps extends React.HTMLAttributes<HTMLHeadingElement> {
  tag: "h1" | "h2" | "h3" | "h4" | "h5" | "h6";
  children?: ReactNode;
}
export const Heading = ({ children, tag: Tag, ...props }: HeadingProps) => {
  const linkRef = useRef<HTMLAnchorElement>(null);

  return (
    <Tag {...props} className="group">
      <a
        href={`#${props.id}`}
        className="no-underline float-left absolute -ml-[1.25em] hidden sm:block opacity-0 group-hover:opacity-100 transition-opacity"
        aria-label="Navigate to header"
        ref={linkRef}
        data-pagefind-ignore
      >
        <span aria-hidden="true">#</span>
      </a>

      <span className="cursor-pointer" onClick={() => linkRef.current?.click()}>
        {children}
      </span>
    </Tag>
  );
};
