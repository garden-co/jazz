import DocsLayout from "@/components/docs/DocsLayout";
import { DocNav } from "@/components/docs/DocsNav";
import { HelpLinks } from "@/components/docs/HelpLinks";
import { PreviousNextLinks } from "@/components/docs/PreviousNextLinks";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import { Toc } from "@stefanprobst/rehype-extract-toc";
import { error } from "console";

export function DocProse({ children }: { children: React.ReactNode }) {
  return (
    <Prose className="overflow-hidden pb-8 pt-[calc(61px+2rem)] md:pt-8 md:max-w-3xl mx-auto">
      {children}
    </Prose>
  );
}

/**
 * Dynamically import MDX module.
 * Tries framework-specific first, then vanilla fallback.
 */
export async function getDocModule(framework: string, slug?: string[]) {
  const slugPath = slug?.join("/");

  // First try framework-specific MDX
  if (slugPath) {
    try {
      return await import(`../content/docs/${slugPath}/${framework}.mdx`);
    } catch { }
  }

  // Fallback to generic MDX
  if (slugPath) {
    try {
      return await import(`../content/docs/${slugPath}.mdx`);
    } catch { }
  }

  // Top-level index fallback
  try {
    return await import(`../content/docs/index.mdx`);
  } catch { }

  return null;
}


/**
 * Get content and TOC for the page
 */
export async function getMdxWithToc(framework: string, slug?: string[]) {
  const mdxModule = await getDocModule(framework, slug);

  if (!mdxModule) {
    throw new Error("MDX file not found");
  }

  const Content = mdxModule.default;
  const tableOfContents = mdxModule.tableOfContents ?? [];
  const headingsFrameworkVisibility = mdxModule.headingsFrameworkVisibility ?? {};
  const ex = mdxModule.metadata ?? {};

  const tocItems = filterTocItemsForFramework(tableOfContents, framework, headingsFrameworkVisibility);

  return { Content, tocItems, ex };
}

/**
 * Filter TOC items based on framework visibility
 */
function filterTocItemsForFramework(
  tocItems: Toc,
  framework: string,
  headingsFrameworkVisibility: Record<string, string[]>
): Toc {
  return tocItems
    .map((item) => {
      const isVisible =
        !item.id ||
        !(item.id in headingsFrameworkVisibility) ||
        headingsFrameworkVisibility[item.id]?.includes(framework);

      if (!isVisible) return null;

      const filteredChildren = item.children
        ? filterTocItemsForFramework(item.children, framework, headingsFrameworkVisibility)
        : [];

      return { ...item, children: filteredChildren };
    })
    .filter(Boolean) as Toc;
}

/**
 * Get page metadata, including frontmatter and exported metadata
 */
export async function getDocMetadata(framework: string, slug?: string[]) {
  const mdxModule = await getDocModule(framework, slug ?? []);
  // Fallback metadata if no MDX file found
  if (!mdxModule) {
    const topic = slug?.[0] ?? "";
    const subtopic = slug?.[1] ?? "";
    const capitalizedFramework = framework
      .split("-")
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ");

    return {
      fm: {
        title: topic
          ? `${capitalizedFramework} Docs: ${topic}${subtopic ? " / " + subtopic : ""}`
          : `${capitalizedFramework} Docs`,
        description: `Documentation for ${capitalizedFramework}`,
        image: "/jazz-logo.png",
        topic,
        subtopic,
      },
      ex: {},
    };
  }

  const fm = mdxModule.frontmatter ?? {};
  const ex = mdxModule.metadata ?? {};
  const firstTocItem = mdxModule.tableOfContents?.[0];
  const titleFromToc = firstTocItem?.value;

  const capitalizedFramework = framework
    .split("-")
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");

  const title = titleFromToc ?? fm.title ?? `${capitalizedFramework} Docs`;
  const description = fm.description ?? ex.description ?? `Documentation for ${capitalizedFramework}`;
  const image = fm.image ?? ex.image ?? "/jazz-logo.png";

  const topic = slug?.[0] ?? "";
  const subtopic = slug?.[1] ?? "";

  return {
    fm: { title, description, image, topic, subtopic },
    ex,
  };
}

/**
 * Page component
 */
export async function DocPage({ framework, slug }: { framework: string; slug?: string[] }) {
  try {
    const { Content, tocItems } = await getMdxWithToc(framework, slug);
    return (
      <DocsLayout nav={<DocNav />} tocItems={tocItems} pagefindLowPriority={slug?.length ? slug[0] === "upgrade" : false}>
        <DocProse>
          <Content />
          <div className="divide-y mt-12">
            <HelpLinks className="lg:hidden pb-4" />
            <PreviousNextLinks slug={slug} framework={framework} />
          </div>
        </DocProse>
      </DocsLayout>
    );
  } catch (err) {
    console.error("Error loading MDX:", err);
    const { default: ComingSoon } = await import("../content/docs/coming-soon.mdx");
    return (
      <DocsLayout nav={<DocNav />} tocItems={[]} pagefindIgnore>
        <DocProse>
          <ComingSoon />
        </DocProse>
      </DocsLayout>
    );
  }
}

/**
 * Generate OG metadata for a page
 */
export function generateOGMetadata(
  framework: string,
  slug: string[],
  docMeta: { title: string; description: string; image?: string; topic?: string; subtopic?: string }
) {
  const { title, description, image, topic, subtopic } = docMeta;
  const baseUrl = "https://jazz.tools";
  const imageUrl = image
    ? `${baseUrl}/api/opengraph-image?title=${encodeURIComponent(title)}&framework=${encodeURIComponent(
      framework
    )}${topic ? `&topic=${encodeURIComponent(topic)}` : ""}${subtopic ? `&subtopic=${encodeURIComponent(subtopic)}` : ""}`
    : "/jazz-logo.png";

  return {
    title,
    description,
    openGraph: {
      title,
      description,
      type: "article",
      url: `/docs/${[framework, ...slug].join("/")}`,
      images: [{ url: imageUrl, width: 1200, height: 630, alt: title }],
    },
    twitter: { card: "summary_large_image", title, description, images: [imageUrl] },
  };
}
