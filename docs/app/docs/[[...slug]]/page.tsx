import type { ReactNode } from "react";
import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { findNeighbour } from "fumadocs-core/page-tree";
import { ClickableCard } from "@astryxdesign/core/ClickableCard";
import { Divider } from "@astryxdesign/core/Divider";
import { Layout, LayoutContent, LayoutPanel } from "@astryxdesign/core/Layout";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { getPageImage, source } from "@/lib/source";
import { textOf } from "@/lib/docs-nav";
import { getDocsMDXComponents } from "@/components/docs/mdx";
import { DocsLink } from "@/components/docs/mdx-client";
import { DocsToc } from "@/components/docs/docs-toc";
import { LLMCopyButton, ViewOptions } from "@/components/ai/page-actions";
import { gitConfig } from "@/lib/layout.shared";

function NeighbourCard({
  direction,
  item,
}: {
  direction: "Previous" | "Next";
  item?: { name: ReactNode; url: string };
}) {
  if (!item) return <div />;
  const name = textOf(item.name);
  return (
    <ClickableCard label={`${direction}: ${name}`} href={item.url} padding={3}>
      <VStack gap={0.5}>
        <Text type="supporting" color="secondary" display="block">
          {direction}
        </Text>
        <Text weight="medium" display="block">
          {name}
        </Text>
      </VStack>
    </ClickableCard>
  );
}

export default async function Page(props: PageProps<"/docs/[[...slug]]">) {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  const current = page;
  const MDX = page.data.body;
  const neighbours = findNeighbour(source.getPageTree(), page.url);
  const toc = page.data.toc.map((item) => ({
    id: item.url.replace(/^#/, ""),
    label: textOf(item.title),
    level: item.depth,
  }));

  // Resolve relative `.mdx` links against this page, then render them as
  // Astryx links.
  async function RelativeLink({ href, children }: { href?: string; children?: ReactNode }) {
    return <DocsLink href={href ? source.resolveHref(href, current) : href}>{children}</DocsLink>;
  }

  return (
    <Layout
      height="auto"
      contentWidth={1120}
      end={
        page.data.full ? undefined : (
          <LayoutPanel
            isScrollable={false}
            label="On this page"
            role="complementary"
            width={240}
            className="docs-toc-panel"
          >
            <DocsToc items={toc} />
          </LayoutPanel>
        )
      }
      content={
        <LayoutContent isScrollable={false} padding={8}>
          <article className="docs-body">
            <VStack gap={2}>
              <Heading level={1}>{page.data.title}</Heading>
              {page.data.description && (
                <Text type="large" color="secondary" display="block">
                  {page.data.description}
                </Text>
              )}
              <HStack gap={2} vAlign="center">
                <LLMCopyButton markdownUrl={`${page.url}.mdx`} />
                <ViewOptions
                  markdownUrl={`${page.url}.mdx`}
                  githubUrl={`https://github.com/${gitConfig.user}/${gitConfig.repo}/blob/${gitConfig.branch}/content/docs/${page.path}`}
                />
              </HStack>
            </VStack>
            <Divider className="my-8" />
            <MDX components={getDocsMDXComponents({ a: RelativeLink })} />
            <Divider className="my-10" />
            <nav aria-label="Pagination" className="grid gap-3 sm:grid-cols-2">
              <NeighbourCard direction="Previous" item={neighbours.previous} />
              <div className="sm:text-end">
                <NeighbourCard direction="Next" item={neighbours.next} />
              </div>
            </nav>
          </article>
        </LayoutContent>
      }
    />
  );
}

export async function generateStaticParams() {
  return source.generateParams();
}

export async function generateMetadata(props: PageProps<"/docs/[[...slug]]">): Promise<Metadata> {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  return {
    title: page.data.title,
    description: page.data.description,
    openGraph: {
      images: getPageImage(page).url,
    },
  };
}
