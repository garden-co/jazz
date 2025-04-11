import { DocNav } from "@/components/docs/nav";
import { JazzNav } from "@/components/nav";
import { clsx } from "clsx";
import type { NavSection } from "gcmp-design-system/src/app/components/organisms/Nav";

export default function Layout({ children }: { children: React.ReactNode }) {
  const navSections: NavSection[] = [
    {
      name: "Docs",
      content: <DocNav />,
      icon: "docs",
    },
  ];

  return (
    <div className="flex-1 w-full">
      <JazzNav sections={navSections} />
      <main>
        <div className="container relative md:grid md:grid-cols-12 md:gap-12">
          <div
            className={clsx(
              "pr-3 md:col-span-4 lg:col-span-3",
              "sticky align-start top-[61px] h-[calc(100vh-61px)] overflow-y-auto",
              "hidden md:block",
            )}
          >
            <DocNav />
          </div>
          <div className={clsx("md:col-span-8 lg:col-span-9 flex gap-12")}>
            {children}
          </div>
        </div>
      </main>
    </div>
  );
}
