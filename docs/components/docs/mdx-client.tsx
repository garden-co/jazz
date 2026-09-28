"use client";

import type { ReactNode } from "react";
import { Banner } from "@astryxdesign/core/Banner";
import { ClickableCard } from "@astryxdesign/core/ClickableCard";
import { CodeBlock } from "@astryxdesign/core/CodeBlock";
import { Collapsible, CollapsibleGroup } from "@astryxdesign/core/Collapsible";
import { Grid } from "@astryxdesign/core/Grid";
import { Link } from "@astryxdesign/core/Link";
import { VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { codeLanguage, customTokenizer } from "@/lib/code-tokenizers";

export function DocsLink({ href, children }: { href?: string; children?: ReactNode }) {
  const external = href != null && /^[a-z]+:/i.test(href);
  return (
    <Link
      href={href ?? "#"}
      {...(external ? { target: "_blank", rel: "noreferrer noopener" } : {})}
    >
      {children}
    </Link>
  );
}

/** Code for docs pages; its props come from `rehypeCodeMeta`. */
export function Code({
  code,
  language,
  title,
  isScrollable = true,
}: {
  code: string;
  language?: string;
  title?: string;
  isScrollable?: boolean;
}) {
  const lang = codeLanguage(language);
  return (
    <CodeBlock
      code={code}
      language={lang}
      title={title}
      tokenizer={customTokenizer(lang)}
      hasLanguageLabel={false}
      width="100%"
      maxHeight={isScrollable ? 600 : undefined}
      className="docs-block"
    />
  );
}

export function CodeBlockPre({
  code,
  language,
  title,
  custom,
  children,
}: {
  code?: string;
  language?: string;
  title?: string;
  custom?: string;
  children?: ReactNode;
}) {
  if (code == null) return <pre>{children}</pre>;
  return (
    <Code code={code} language={language} title={title} isScrollable={custom !== "noscroll"} />
  );
}

const CALLOUT_STATUS = {
  info: "info",
  warn: "warning",
  warning: "warning",
  error: "error",
  success: "success",
  idea: "info",
} as const;

const CALLOUT_TITLE = {
  info: "Note",
  warning: "Warning",
  error: "Error",
  success: "Tip",
} as const;

/** MDX `<Callout type title>` as an Astryx `Banner`. */
export function Callout({
  type = "info",
  title,
  children,
}: {
  type?: keyof typeof CALLOUT_STATUS;
  title?: ReactNode;
  children?: ReactNode;
}) {
  const status = CALLOUT_STATUS[type] ?? "info";
  return (
    <Banner
      status={status}
      title={title ?? CALLOUT_TITLE[status]}
      description={<div className="docs-callout">{children}</div>}
      className="docs-block"
    />
  );
}

/** MDX `<Accordions>` as an Astryx `CollapsibleGroup`. */
export function Accordions({
  type = "single",
  children,
}: {
  type?: "single" | "multiple";
  children?: ReactNode;
}) {
  return (
    <div className="docs-block">
      <CollapsibleGroup type={type} hasDividers>
        {children}
      </CollapsibleGroup>
    </div>
  );
}

export function Accordion({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <Collapsible value={title} trigger={title} defaultIsOpen={false}>
      <div className="docs-callout">{children}</div>
    </Collapsible>
  );
}

/** MDX `<Cards>` as a responsive grid of Astryx `ClickableCard`s. */
export function Cards({ children }: { children?: ReactNode }) {
  return (
    <div className="docs-block">
      <Grid columns={{ minWidth: 220 }} gap={3}>
        {children}
      </Grid>
    </div>
  );
}

export function Card({
  title,
  description,
  href,
  children,
}: {
  title: string;
  description?: ReactNode;
  href?: string;
  children?: ReactNode;
}) {
  return (
    <ClickableCard label={title} href={href}>
      <VStack gap={1}>
        <Text weight="semibold" display="block">
          {title}
        </Text>
        {(description ?? children) && (
          <Text color="secondary" display="block">
            {description ?? children}
          </Text>
        )}
      </VStack>
    </ClickableCard>
  );
}
