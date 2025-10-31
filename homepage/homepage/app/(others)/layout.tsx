import { JazzFooter } from "@/components/footer";
import { JazzNav } from "@/components/nav";
import { PagefindSearch } from "@/components/pagefind";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <>
    <div className="w-full flex-1 dark:bg-stone-925 dark:bg-transparent">
      <JazzNav />
      <main>{children}</main>
    </div>
    <JazzFooter  />
    <PagefindSearch />
    </>
  );
}
