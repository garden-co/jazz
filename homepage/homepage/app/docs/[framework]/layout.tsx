import { SideNavLayout } from "@/components/SideNavLayout";
import { DocNav } from "@/components/docs/DocsNav";

export default function Layout({ children }: { children: React.ReactNode }) {
  return <SideNavLayout sideNav={<DocNav />}>{children}</SideNavLayout>;
}
