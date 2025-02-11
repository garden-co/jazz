import DocsLayout from "@/components/docs/DocsLayout";
import { DocNav } from "@/components/docs/nav";
import { Prose } from "gcmp-design-system/src/app/components/molecules/Prose";

export default function Layout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <DocsLayout nav={<DocNav />}>
      <Prose className="max-w-3xl mx-auto lg:flex-1 py-10">{children}</Prose>
    </DocsLayout>
  );
}
