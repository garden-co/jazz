import DocsLayout from "@/components/docs/DocsLayout";
import { DocNav } from "@/components/docs/DocsNav";
import { HelpLinks } from "@/components/docs/HelpLinks";
import { PreviousNextLinks } from "@/components/docs/PreviousNextLinks";
import { Separator } from "@garden-co/design-system/src/components/atoms/Separator";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import { Toc } from "@stefanprobst/rehype-extract-toc";

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

export async function getDocMetadata(framework: string, slug?: string[]) {
  const slugPath = slug?.join("/");

  try {
    const mdxSource = await getMdxSource(framework, slugPath);

    const title =
      mdxSource.metadata.title ||
      mdxSource.tableOfContents?.[0].value ||
      "Documentation";

    return {
      title,
      description: mdxSource.metadata.description,
      openGraph: {
        title,
      },
    };
  } catch (error) {
    const title = "Documentation";
    return {
      title,
      openGraph: {
        title,
      },
    };
  }
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
      <DocsLayout nav={<DocNav />} tocItems={tocItems}>
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
    return (
      <DocsLayout nav={<DocNav />} tocItems={[]}>
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
