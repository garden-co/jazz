import { source } from "@/lib/source";
import { docsNavSections } from "@/lib/docs-nav";
import { SiteShell } from "@/components/site/site-shell";
import { DocsSideNav } from "@/components/docs/docs-side-nav";
import { CodeCopyCleaner } from "@/components/docs/code-copy-cleaner";

export default function Layout({ children }: LayoutProps<"/docs">) {
  return (
    <SiteShell sideNav={<DocsSideNav sections={docsNavSections(source.getPageTree())} />}>
      <CodeCopyCleaner />
      {children}
    </SiteShell>
  );
}
