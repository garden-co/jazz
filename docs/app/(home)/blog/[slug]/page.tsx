import { getMDXComponents } from "@/mdx-components";
import { blogSource } from "@/lib/source";
import { DocsBody } from "fumadocs-ui/layouts/docs/page";
import { InlineTOC } from "fumadocs-ui/components/inline-toc";
import { createRelativeLink } from "fumadocs-ui/mdx";
import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";

const dateFormatter = new Intl.DateTimeFormat("en-US", {
  dateStyle: "long",
});

export default async function BlogPostPage(props: { params: Promise<{ slug: string }> }) {
  const params = await props.params;
  const page = blogSource.getPage([params.slug]);

  if (!page) notFound();

  const MDX = page.data.body;

  return (
    <div className="w-full">
      <article className="mx-auto w-full max-w-(--fd-layout-width) px-4 pb-24 pt-18 sm:pb-28 sm:pt-22 lg:pb-32 lg:pt-26">
        <div className="max-w-[42rem] space-y-5">
          <Link
            href="/blog"
            className="inline-flex text-sm font-medium text-fd-muted-foreground transition-colors hover:text-fd-foreground"
          >
            Blog
          </Link>
          <div className="space-y-3">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
              {dateFormatter.format(new Date(page.data.date))}
            </p>
            <h1 className="text-[clamp(3rem,7vw,5.5rem)] font-black leading-[0.92] tracking-[-0.05em]">
              {page.data.title}
            </h1>
            <p className="max-w-[38rem] text-lg leading-relaxed text-fd-muted-foreground sm:text-xl">
              {page.data.description}
            </p>
            <p className="text-sm font-medium">By {page.data.author}</p>
          </div>
        </div>
        <div className="mt-14 grid gap-12 lg:grid-cols-[minmax(0,1fr)_15rem] lg:items-start">
          <div className="min-w-0 max-w-[48rem]">
            <DocsBody>
              <MDX
                components={getMDXComponents({
                  a: createRelativeLink(blogSource, page),
                })}
              />
            </DocsBody>
          </div>
          {page.data.toc.length > 0 ? (
            <aside className="hidden lg:sticky lg:top-20 lg:block">
              <div className="border-t pt-4">
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                  On this page
                </p>
                <div className="mt-4 text-sm">
                  <InlineTOC items={page.data.toc} />
                </div>
              </div>
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
