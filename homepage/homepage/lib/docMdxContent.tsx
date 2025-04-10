import { ComingSoon } from "@/components/docs/ComingSoon";
import DocsLayout from "@/components/docs/DocsLayout";
import { DocNav } from "@/components/docs/nav";
import { Toc } from "@stefanprobst/rehype-extract-toc";
import { Prose } from "gcmp-design-system/src/app/components/molecules/Prose";

export async function getMdxSource(framework: string, slugPath?: string) {
  // Try to import the framework-specific file first
  try {
    if (!slugPath) {
      return await import("../content/docs/index.mdx");
    }
    return await import(`../content/docs/${slugPath}/${framework}.mdx`);
  } catch (error) {
    // Fallback to vanilla
    return await import(`../content/docs/${slugPath}.mdx`);
  }
}

export async function getDocMetadata(framework: string, slug?: string[]) {
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

export async function DocPage({
  framework,
  slug,
}: {
  framework: string;
  slug?: string[];
}) {
  const slugPath = slug?.join("/");

  try {
    const mdxSource = await getMdxSource(framework, slugPath);

    const {
      default: Content,
      tableOfContents,
      headingsFrameworkVisibility,
      test,
    } = mdxSource;

    // Remove items that should not be shown for the current framework
    const tocItems = (tableOfContents as Toc).filter(({ id }) =>
      id && id in headingsFrameworkVisibility
        ? headingsFrameworkVisibility[id]?.includes(framework)
        : true,
    );

    return (
      <DocsLayout nav={<DocNav />} tocItems={tocItems}>
        <Prose className="overflow-x-hidden lg:flex-1 py-10  max-w-3xl mx-auto">
          <Content />
        </Prose>
      </DocsLayout>
    );
  } catch (error) {
    return (
      <DocsLayout nav={<DocNav />} tocItems={[]}>
        <Prose className="overflow-x-hidden lg:flex-1 py-10  max-w-3xl mx-auto">
          <ComingSoon />
        </Prose>
      </DocsLayout>
    );
  }
}
