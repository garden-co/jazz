import { SideNavLayout } from "@/components/SideNavLayout";
import { ApiNav } from "@/components/docs/ApiNav";
import { JazzMobileNav } from "@/components/nav";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <SideNavLayout
      sideNav={<ApiNav />}
      floatingNavSections={[
        {
          name: "API Ref",
          content: <ApiNav />,
          icon: "package",
        },
      ]}
    >
      <Prose className="overflow-x-hidden lg:flex-1 pb-10 pt-[calc(61px+2.5rem)] md:pt-10">
        {children}
      </Prose>

      <JazzMobileNav
        sections={[
          {
            name: "API Ref",
            content: <ApiNav />,
            icon: "package",
          },
        ]}
      />
    </SideNavLayout>
  );
}
