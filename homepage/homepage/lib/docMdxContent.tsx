import DocsLayout from "@/components/docs/DocsLayout";
import { DocNav } from "@/components/docs/DocsNav";
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
  const tocItems = (tableOfContents as Toc).filter(({ id }) =>
    id && id in headingsFrameworkVisibility
      ? headingsFrameworkVisibility[id]?.includes(framework)
      : true,
  );

  return {
    Content,
    tocItems,
  };
}
