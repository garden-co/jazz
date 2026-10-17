import type { ReactNode } from "react";
import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { ArrowLeft, ArrowRight } from "lucide-react";
import { ClickableCard } from "@astryxdesign/core/ClickableCard";
import { Divider } from "@astryxdesign/core/Divider";
import { Heading } from "@astryxdesign/core/Heading";
import { Link } from "@astryxdesign/core/Link";
import { VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { getDocsMDXComponents, heading } from "@/components/docs/mdx";
import { DocsLink } from "@/components/docs/mdx-client";
import { DocsToc } from "@/components/docs/docs-toc";
import { textOf } from "@/lib/docs-nav";
import { blogSource } from "@/lib/source";

const dateFormatter = new Intl.DateTimeFormat("en-US", {
  dateStyle: "long",
});

function sortedPosts() {
  return [...blogSource.getPages()].sort(
    (left, right) => new Date(right.data.date).getTime() - new Date(left.data.date).getTime(),
  );
}

function PostCard({
  direction,
  post,
}: {
  direction: "Newer" | "Older";
  post?: { url: string; data: { title: string } };
}) {
  if (!post) return <div />;
  return (
    <ClickableCard label={`${direction} post: ${post.data.title}`} href={post.url} padding={3}>
      <VStack gap={0.5}>
        <Text type="supporting" color="secondary" display="block">
          <span
            className={`inline-flex items-center gap-1.5 ${direction === "Older" ? "flex-row-reverse" : ""}`}
          >
            {direction === "Older" ? (
              <ArrowRight aria-hidden className="size-3.5" />
            ) : (
              <ArrowLeft aria-hidden className="size-3.5" />
            )}
            {direction} post
          </span>
        </Text>
        <Text weight="medium" display="block">
          {post.data.title}
        </Text>
      </VStack>
    </ClickableCard>
  );
}

export default async function BlogPostPage(props: { params: Promise<{ slug: string }> }) {
  const params = await props.params;
  const page = blogSource.getPage([params.slug]);

  if (!page) notFound();

  const current = page;
  const MDX = page.data.body;
  const posts = sortedPosts();
  const index = posts.findIndex((post) => post.url === page.url);
  const toc = page.data.toc.map((item) => ({
    id: item.url.replace(/^#/, ""),
    label: textOf(item.title),
    level: item.depth + 1,
  }));

  // Resolve relative `.mdx` links against this post, then render them as
  // Astryx links.
  async function RelativeLink({ href, children }: { href?: string; children?: ReactNode }) {
    return (
      <DocsLink href={href ? blogSource.resolveHref(href, current) : href}>{children}</DocsLink>
    );
  }

  return (
    <div className="w-full">
      <article className="home-container pb-24 pt-16 sm:pt-20">
        <header className="blog-post-header">
          <Text as="p" display="block" color="secondary">
            <Link href="/blog" color="inherit">
              Blog
            </Link>
          </Text>
          <Heading level={1} type="display-2" className="mt-4">
            {page.data.title}
          </Heading>
          {page.data.description ? (
            <Text
              as="p"
              display="block"
              type="large"
              color="secondary"
              weight="normal"
              className="mt-4"
            >
              {page.data.description}
            </Text>
          ) : null}
          <Text as="p" display="block" type="supporting" color="secondary" className="mt-6">
            {page.data.author} ·{" "}
            <time dateTime={new Date(page.data.date).toISOString().slice(0, 10)}>
              {dateFormatter.format(new Date(page.data.date))}
            </time>
          </Text>
        </header>
        <Divider className="my-10" />
        <div className="blog-post-layout">
          <div className="docs-body blog-post-body">
            <MDX
              components={getDocsMDXComponents({
                a: RelativeLink,
                // The post title is the page's h1, so section headings in the
                // post start at h2.
                h1: heading(2),
                h2: heading(3),
                h3: heading(4),
                h4: heading(5),
                h5: heading(6),
              })}
            />
            <Divider className="my-10" />
            <nav aria-label="More posts" className="grid gap-3 sm:grid-cols-2">
              <PostCard direction="Newer" post={index > 0 ? posts[index - 1] : undefined} />
              <div className="sm:text-end">
                <PostCard
                  direction="Older"
                  post={index >= 0 && index < posts.length - 1 ? posts[index + 1] : undefined}
                />
              </div>
            </nav>
          </div>
          {toc.length > 0 ? (
            <aside className="blog-post-toc">
              <DocsToc items={toc} />
            </aside>
          ) : null}
        </div>
      </article>
    </div>
  );
}

export function generateStaticParams(): { slug: string }[] {
  return blogSource.getPages().map((page) => ({
    slug: page.slugs[0],
  }));
}

export async function generateMetadata(props: {
  params: Promise<{ slug: string }>;
}): Promise<Metadata> {
  const params = await props.params;
  const page = blogSource.getPage([params.slug]);

  if (!page) notFound();

  return {
    title: page.data.title,
    description: page.data.description,
  };
}
