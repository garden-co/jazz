import type { ComponentProps, ReactNode } from "react";
import type { MDXComponents } from "mdx/types";
import { Heading, Text } from "@astryxdesign/core/Text";
import { Code as InlineCode } from "@astryxdesign/core/Code";
import { Blockquote } from "@astryxdesign/core/Blockquote";
import { Divider } from "@astryxdesign/core/Divider";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableHeaderCell,
  TableRow,
} from "@astryxdesign/core/Table";
import { getMDXComponents } from "@/mdx-components";
import { Accordion, Accordions, Callout, Card, Cards, CodeBlockPre, DocsLink } from "./mdx-client";

type HeadingProps = ComponentProps<"h2">;

function heading(level: 1 | 2 | 3 | 4 | 5 | 6) {
  return function DocsHeading({ id, children }: HeadingProps) {
    return (
      <Heading level={level} id={id} className="docs-heading">
        {children}
      </Heading>
    );
  };
}

function Paragraph({ children }: { children?: ReactNode }) {
  return (
    <Text as="p" display="block" className="docs-block">
      {children}
    </Text>
  );
}

/**
 * MDX components for docs pages, built from Astryx components under the
 * Jazz theme. The slide and diagram components shared with the blog and
 * presentations come from the site-wide map; the docs override prose,
 * code, tables, callouts, tabs, accordions and cards.
 */
export function getDocsMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...getMDXComponents(),
    h1: heading(1),
    h2: heading(2),
    h3: heading(3),
    h4: heading(4),
    h5: heading(5),
    h6: heading(6),
    p: Paragraph,
    a: DocsLink,
    code: ({ children }: { children?: ReactNode }) => <InlineCode>{children}</InlineCode>,
    pre: CodeBlockPre,
    blockquote: ({ children }: { children?: ReactNode }) => (
      <Blockquote className="docs-block">{children}</Blockquote>
    ),
    hr: () => <Divider className="docs-block" />,
    table: ({ children }: { children?: ReactNode }) => (
      <div className="docs-block overflow-x-auto">
        <Table density="compact" verticalAlign="top">
          {children}
        </Table>
      </div>
    ),
    thead: ({ children }: { children?: ReactNode }) => <TableHeader>{children}</TableHeader>,
    tbody: ({ children }: { children?: ReactNode }) => <TableBody>{children}</TableBody>,
    tr: ({ children }: { children?: ReactNode }) => <TableRow>{children}</TableRow>,
    th: ({ children }: { children?: ReactNode }) => <TableHeaderCell>{children}</TableHeaderCell>,
    td: ({ children }: { children?: ReactNode }) => <TableCell>{children}</TableCell>,
    Callout,
    Accordions,
    Accordion,
    Cards,
    Card,
    ...components,
  };
}
