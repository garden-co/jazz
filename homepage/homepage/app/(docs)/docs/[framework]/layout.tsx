import { SideNavLayout } from "@/components/SideNavLayout";
import { DocNav } from "@/components/docs/DocsNav";
import { JazzFooter } from "@/components/footer";
import { PagefindSearch } from "@/components/pagefind";

export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <>
      <SideNavLayout sideNav={<DocNav />}>{children}</SideNavLayout>
      <JazzFooter />
      <PagefindSearch />{" "}
    </>
  );
}
