import DocsLayout from "@/components/docs/DocsLayout";
import { DocNav } from "@/components/docs/DocsNav";
import { HelpLinks } from "@/components/docs/HelpLinks";
import { PreviousNextLinks } from "@/components/docs/PreviousNextLinks";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import { Toc } from "@stefanprobst/rehype-extract-toc";
import fs, { readFileSync } from "fs";
import path, { join } from "path";
import matter from "gray-matter";

export async function getMdxSource(framework: string, slugPath?: string) {
  // Try to import the framework-specific file first
  try {
    if (!slugPath) {
      return await import("../content/docs/index.mdx");
    }
    return await import(`../content/docs/${slugPath}/${framework}.mdx`);
  } catch (error) {
    // Fallback to vanilla
    console.log(`Falling back to vanilla for ${slugPath}`);
    return await import(`../content/docs/${slugPath}.mdx`);
  }
}

export type DocMetadata = {
  title: string;
  description: string;
  image: string;
};

export function getDocBySlug(framework: string, slug: string[]) {
  const baseDir = path.join(process.cwd(), "content", "docs", framework);

  let filePath: string;

  if (slug.length === 0) {
    // framework-level
    filePath = path.join(baseDir, "index.mdx");
  } else if (slug.length === 1) {
    // topic-level 
    filePath = path.join(baseDir, slug[0], "index.mdx");
  } else if (slug.length === 2) {
    // subtopic-level 
    filePath = path.join(baseDir, slug[0], `${slug[1]}.mdx`);
  } else {
    // unsupported depth
    return undefined;
  }

  if (!fs.existsSync(filePath)) return undefined;

  const source = fs.readFileSync(filePath, "utf-8");
  const { data: frontmatter } = matter(source);

  return { frontmatter, source };
}

export function getDocMetadata(framework:string, slugPath: string[]) {
  const mdxSource = getDocBySlug(framework, slugPath);

    if (!mdxSource) {
    return {
      title: `${framework} Docs`,
      description: `Documentation for ${framework}`,
      image: "/jazz-logo.png",
      topic: slugPath[0] ?? "",
      subtopic: slugPath[1] ?? "",
    };
  }

  return {
    title: mdxSource.frontmatter.title ?? `${framework} Docs`,
    description: mdxSource.frontmatter.description ?? `Documentation for ${framework}`,
    image: mdxSource.frontmatter.image ?? "/jazz-logo.png",
    topic: slugPath[0] ?? "",
    subtopic: slugPath[1] ?? "",
  };
}


function DocProse({ children }: { children: React.ReactNode }) {
  return (
    <Prose className="overflow-hidden pb-8 pt-[calc(61px+2rem)] md:pt-8 md:max-w-3xl mx-auto">
      {children}
    </Prose>
  );
}

export async function DocPage({
  framework,
  slug,
}: {
  framework: string;
  slug?: string[];
}) {
  try {
    const { Content, tocItems } = await getMdxWithToc(framework, slug);

    return (
      <DocsLayout
        nav={<DocNav />}
        tocItems={tocItems}
        pagefindLowPriority={slug?.length ? slug[0] === "upgrade" : false}
      >
        <DocProse>
          <Content />

          <div className="divide-y mt-12">
            <HelpLinks className="lg:hidden pb-4" />

            <PreviousNextLinks slug={slug} framework={framework} />
          </div>
        </DocProse>
      </DocsLayout>
    );
  } catch (error) {
    const { default: ComingSoon } = await import(
      "../content/docs/coming-soon.mdx"
    );
    console.error("Error loading MDX:", error);
    return (
      <DocsLayout nav={<DocNav />} tocItems={[]} pagefindIgnore>
        <DocProse>
          <ComingSoon />
        </DocProse>
      </DocsLayout>
    );
  }
}

export async function getMdxWithToc(framework: string, slug?: string[]) {
  const slugPath = slug?.join("/");
  const mdxSource = await getMdxSource(framework, slugPath);

  const {
    default: Content,
    tableOfContents,
    headingsFrameworkVisibility,
  } = mdxSource;

  // Remove items that should not be shown for the current framework
  const tocItems = filterTocItemsForFramework(
    tableOfContents as Toc,
    framework,
    headingsFrameworkVisibility,
  );

  return {
    Content,
    tocItems,
  };
}

function filterTocItemsForFramework(
  tocItems: Toc,
  framework: string,
  headingsFrameworkVisibility: Record<string, string[]>,
): Toc {
  return tocItems
    .map((item) => {
      const isVisible =
        !item.id ||
        !(item.id in headingsFrameworkVisibility) ||
        headingsFrameworkVisibility[item.id]?.includes(framework);

      if (!isVisible) return null;

      const filteredChildren = item.children
        ? filterTocItemsForFramework(
            item.children,
            framework,
            headingsFrameworkVisibility,
          )
        : [];

      return {
        ...item,
        children: filteredChildren,
      };
    })
    .filter(Boolean) as Toc;
}

export function generateOGMetadata(
  framework: string,
  slug: string[],
  docMeta: { title: string; description: string; image?: string; topic?: string; subtopic?: string }
) {
  const { title, description, image, topic, subtopic } = docMeta;
  const baseUrl = "https://jazz.tools";
  const imageUrl = image
  ? `${baseUrl}/opengraph-image?title=${encodeURIComponent(title)}&framework=${encodeURIComponent(framework)}${topic ? `&topic=${encodeURIComponent(topic)}` : ""}${subtopic ? `&subtopic=${encodeURIComponent(subtopic)}` : ""}`
  : "/jazz-logo.png";

  return {
    title,
    description,
    openGraph: {
      title,
      description,
      type: "article",
      url: `https://jazz.tools/docs/${[framework, ...slug].join("/")}`,
      images: [
        {
          url: imageUrl,
          width: 1200,
          height: 630,
          alt: title,
        },
      ],
    },
    twitter: {
      card: "summary_large_image",
      title,
      description,
      images: [imageUrl],
    },
  };
}

