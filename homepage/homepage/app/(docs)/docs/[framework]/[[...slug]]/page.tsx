import { TocItemsSetter } from "@/components/docs/TocItemsSetter";
import ComingSoonPage from "@/components/docs/coming-soon.mdx";
import { docNavigationItems } from "@/lib/docNavigationItems.js";
import { Framework, frameworks } from "@/lib/framework";
import type { Toc } from "@stefanprobst/rehype-extract-toc";

async function getMdxSource(framework: string, slugPath?: string) {
  // Try to import the framework-specific file first
  try {
    if (!slugPath) {
      return await import("./index.mdx")
    }
    return await import(`./${slugPath}/${framework}.mdx`);
  } catch (error) {
    // Fallback to vanilla
    return await import(`./${slugPath}.mdx`);
  }
}

export async function generateMetadata({
  params,
}: { params: Promise<{ slug: string[]; framework: string }> }) {
  const { slug, framework } = await params;

  const slugPath = slug?.join("/");

  try {
    const mdxSource = await getMdxSource(framework, slugPath);
    const title = mdxSource.tableOfContents?.[0].value || "Documentation";

    return {
      title,
      openGraph: {
        title,
      },
    };
  } catch (error) {
    return {
      title: "Documentation",
      openGraph: {
        title: "Documentation",
      },
    };
  }
}

export default async function Page({
  params,
}: { params: Promise<{ slug: string[]; framework: string }> }) {
  const { slug, framework } = await params;

  const slugPath = slug?.join("/");

  try {
    const mdxSource = await getMdxSource(framework, slugPath);
    const { default: Content, tableOfContents } = mdxSource;

    // Exclude h1 from table of contents
    const tocItems = (tableOfContents as Toc)?.[0]?.children;

    return (
      <>
        <TocItemsSetter items={tocItems} />
        <Content />
      </>
    );
  } catch (error) {
    return (
      <>
        <TocItemsSetter items={[]} />
        <ComingSoonPage />
      </>
    );
  }
}

// https://nextjs.org/docs/app/api-reference/functions/generate-static-params
export const dynamicParams = false;
export const dynamic = "force-static";

export async function generateStaticParams() {
  const paths: Array<{ slug?: string[]; framework: Framework }> = [];

  for (const framework of frameworks) {
    paths.push({
      framework,
      slug: [],
    })
    for (const heading of docNavigationItems) {
      for (const item of heading?.items) {
        if (item.href && item.href.startsWith("/docs")) {
          const slug = item.href
            .replace("/docs", "")
            .split("/")
            .filter(Boolean);
          if (slug.length) {
            paths.push({
              slug,
              framework,
            });
          }
        }
      }
    }
  }

  return paths;
}
