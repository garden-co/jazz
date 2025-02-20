"use client";
import { HashIcon } from "lucide-react";
import { type ReactNode, useRef } from "react";

interface HeadingProps extends React.HTMLAttributes<HTMLHeadingElement> {
  tag: "h1" | "h2" | "h3" | "h4" | "h5" | "h6";
  children?: ReactNode;
}
export const Heading = ({ children, tag: Tag, ...props }: HeadingProps) => {
  const linkRef = useRef<HTMLAnchorElement>(null);

  return (
    <Tag {...props} className="group">
      <div className="absolute">
        <a
          href={`#${props.id}`}
          className="-ml-8 mt-1 flex items-center opacity-0 group-hover:opacity-100 transition-opacity"
          aria-label="Navigate to header"
          ref={linkRef}
        >
          <HashIcon className="w-6 h-6 " />
        </a>
      </div>
      <span className="cursor-pointer" onClick={() => linkRef.current?.click()}>
        {children}
      </span>
    </Tag>
  );
};
