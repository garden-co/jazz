import { JazzNav } from "@/components/nav";
import { NavSection } from "@garden-co/design-system/src/components/organisms/Nav";
import { clsx } from "clsx";

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
    <div className="w-full flex-1">
      <JazzNav sections={floatingNavSections} hideMobileNav />
      <main>
        <div className="container relative md:grid md:grid-cols-12 md:gap-12">
          <div
            className={clsx(
              "pr-3 pt-3 md:col-span-4 md:pt-8 lg:col-span-3",
              "align-start sticky top-[61px] h-[calc(100vh-61px)]",
              "hidden md:block",
            )}
          >
            {sideNav}
          </div>
          <div className={clsx("flex gap-12 md:col-span-8 lg:col-span-9")}>
            {children}
          </div>
        </div>
      </main>
    </div>
  );
}
