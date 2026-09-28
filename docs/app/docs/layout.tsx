import { source } from "@/lib/source";
import { docsNavSections } from "@/lib/docs-nav";
import { SiteShell } from "@/components/site/site-shell";
import { DocsSideNav } from "@/components/docs/docs-side-nav";

export default function Layout({ children }: LayoutProps<"/docs">) {
  return (
    <SiteShell sideNav={<DocsSideNav sections={docsNavSections(source.getPageTree())} />}>
      {children}
    </SiteShell>
  );
}
