import { JazzNav } from "@/components/nav";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="w-full flex-1 dark:bg-stone-925 dark:bg-transparent">
      <JazzNav />
      <main>{children}</main>
    </div>
  );
}
