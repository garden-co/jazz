import { ApiNav } from "@/components/docs/ApiNav";
import DocsLayout from "@/components/docs/DocsLayout";
import { Prose } from "gcmp-design-system/src/app/components/molecules/Prose";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <DocsLayout nav={<ApiNav />} navIcon="package" navName="API Ref">
      <Prose className="overflow-x-hidden lg:flex-1 py-10">{children}</Prose>
    </DocsLayout>
  );
}
