import type { ReactNode } from "react";
import type { Metadata } from "next";
import { notFound, redirect } from "next/navigation";
import { findNeighbour, type Node } from "fumadocs-core/page-tree";
import { ArrowLeft, ArrowRight } from "lucide-react";
import { ClickableCard } from "@astryxdesign/core/ClickableCard";
import { Divider } from "@astryxdesign/core/Divider";
import { Layout, LayoutContent, LayoutPanel } from "@astryxdesign/core/Layout";
import { VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { getPageImage, source } from "@/lib/source";
import { textOf } from "@/lib/docs-nav";
import { getDocsMDXComponents } from "@/components/docs/mdx";
import { DocsLink } from "@/components/docs/mdx-client";
import { DocsToc } from "@/components/docs/docs-toc";

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
          <span
            className={`inline-flex items-center gap-1.5 ${direction === "Next" ? "flex-row-reverse" : ""}`}
          >
            {direction === "Next" ? (
              <ArrowRight aria-hidden className="size-3.5" />
            ) : (
              <ArrowLeft aria-hidden className="size-3.5" />
            )}
            {direction}
          </span>
        </Text>
        <Text weight="medium" display="block">
          {name}
        </Text>
      </VStack>
    </ClickableCard>
  );
}

/**
 * A folder without an index page (e.g. `/docs/setup/backend`) redirects to
 * its first page, so folder URLs are stable link targets (the agent brief
 * links them).
 */
function firstPageUnder(slug: string[] | undefined): string | undefined {
  if (!slug?.length) return undefined;
  const prefix = `/docs/${slug.join("/")}/`;
  const walk = (nodes: Node[]): string | undefined => {
    for (const node of nodes) {
      if (node.type === "page" && node.url.startsWith(prefix)) return node.url;
      if (node.type === "folder") {
        if (node.index?.url.startsWith(prefix)) return node.index.url;
        const found = walk(node.children);
        if (found) return found;
      }
    }
    return undefined;
  };
  return walk(source.getPageTree().children);
}

function getPageOrRedirect(slug: string[] | undefined) {
  const page = source.getPage(slug);
  if (page) return page;
  const first = firstPageUnder(slug);
  if (first) redirect(first);
  notFound();
}

export default async function Page(props: PageProps<"/docs/[[...slug]]">) {
  const params = await props.params;
  const page = getPageOrRedirect(params.slug);

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
  const page = getPageOrRedirect(params.slug);

  return {
    title: page.data.title,
    description: page.data.description,
    openGraph: {
      images: getPageImage(page).url,
    },
  };
}
