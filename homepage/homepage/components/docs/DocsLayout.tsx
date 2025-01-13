import { TableOfContents } from "@/components/docs/TableOfContents";
import { JazzNav } from "@/components/nav";
import { Toc } from "@stefanprobst/rehype-extract-toc";
import { clsx } from "clsx";

export default function DocsLayout({
  children,
  nav,
  navName,
  navIcon,
  toc,
}: {
  children: React.ReactNode;
  nav?: React.ReactNode;
  navName?: string;
  navIcon?: string;
  toc?: Toc;
}) {
  const navSections = [
    {
      name: navName || "Docs",
      content: nav,
      icon: navIcon || "docs",
    },
    {
      name: "Outline",
      content: toc && (
        <TableOfContents className="text-sm" items={toc as Toc} />
      ),
      icon: "tableOfContents",
    },
  ];

  return (
    <div className="flex-1 w-full">
      <JazzNav sections={navSections} />
      <main>
        <div className="container relative grid grid-cols-12 gap-5">
          <div
            className={clsx(
              "py-8",
              "pr-3 md:col-span-4 lg:col-span-3",
              "sticky align-start top-[65px] h-[calc(100vh-65px)] overflow-y-auto overflow-x-hidden",
              "hidden md:block",
            )}
          >
            {nav}
          </div>
          <div
            className={clsx(
              "col-span-12 md:col-span-8 lg:col-span-9 flex gap-3",
            )}
          >
            {children}
            {toc && (
              <>
                <TableOfContents
                  className="pl-3 py-6 shrink-0 text-sm sticky align-start top-[65px] w-[16rem] h-[calc(100vh-65px)] overflow-y-auto overflow-x-hidden hidden lg:block"
                  items={toc as Toc}
                />
              </>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}
