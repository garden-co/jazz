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

export function heading(level: 1 | 2 | 3 | 4 | 5 | 6) {
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

/** Renders `code` spans and yes/no marks inside a comparison cell. */
function ComparisonText({ text }: { text: string }) {
  if (text === "yes") return <span aria-label="Yes">✓</span>;
  if (text === "no") return <span aria-label="No">✗</span>;
  return (
    <>
      {text
        .split(/(`[^`]+`)/)
        .map((part, i) =>
          part.startsWith("`") && part.endsWith("`") && part.length > 1 ? (
            <InlineCode key={i}>{part.slice(1, -1)}</InlineCode>
          ) : (
            part
          ),
        )}
    </>
  );
}

/**
 * A Jazz-vs-other feature table. Shared traits come first, each with one
 * description spanning both columns; differences follow, one cell per side.
 * A cell of exactly "yes" or "no" renders as a tick or a cross.
 */
function Comparison({
  other,
  common,
  differences,
}: {
  other: string;
  common: [feature: string, description: string][];
  differences: [feature: string, jazz: string, other: string][];
}) {
  return (
    <div className="docs-block docs-table">
      <Table density="compact" verticalAlign="top">
        <TableHeader>
          <TableRow>
            <TableHeaderCell> </TableHeaderCell>
            <TableHeaderCell>Jazz</TableHeaderCell>
            <TableHeaderCell>{other}</TableHeaderCell>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow>
            <TableCell colSpan={3}>
              <strong>In common</strong>
            </TableCell>
          </TableRow>
          {common.map(([feature, description]) => (
            <TableRow key={feature}>
              <TableCell>{feature}</TableCell>
              <TableCell colSpan={2}>
                <ComparisonText text={description} />
              </TableCell>
            </TableRow>
          ))}
          <TableRow>
            <TableCell colSpan={3}>
              <strong>Differences</strong>
            </TableCell>
          </TableRow>
          {differences.map(([feature, jazz, theirs]) => (
            <TableRow key={feature}>
              <TableCell>{feature}</TableCell>
              <TableCell>
                <ComparisonText text={jazz} />
              </TableCell>
              <TableCell>
                <ComparisonText text={theirs} />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
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
      <div className="docs-block docs-table">
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
    Comparison,
    Accordions,
    Accordion,
    Cards,
    Card,
    ...components,
  };
}
