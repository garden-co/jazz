import { JazzNav } from "@/components/nav";
import { clsx } from "clsx";
import { NavSection } from "gcmp-design-system/src/app/components/organisms/Nav";

export function SideNavLayout({
  children,
  sideNav,
  floatingNavSections = [],
}: {
  children: React.ReactNode;
  sideNav: React.ReactNode;
  floatingNavSections?: NavSection[];
}) {
  return (
    <div className="flex-1 w-full">
      <JazzNav sections={floatingNavSections} hideMobileNav />
      <main>
        <div className="container relative md:grid md:grid-cols-12 md:gap-12">
          <div
            className={clsx(
              "pr-3 pt-3 md:pt-8 md:col-span-4 lg:col-span-3",
              "sticky align-start top-[61px] h-[calc(100vh-61px)] overflow-y-auto",
              "hidden md:block",
            )}
          >
            {sideNav}
          </div>
          <div className={clsx("md:col-span-8 lg:col-span-9 flex gap-12")}>
            {children}
          </div>
        </div>
      </main>
    </div>
  );
}
